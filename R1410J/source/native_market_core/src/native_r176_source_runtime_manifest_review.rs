//! Astra R176: exact source/YAML/research-receipt crosswalk.
//! Offline only: never probes the deployed process, private exchange, DB, or
//! network. In particular no historical receipt proves applied PAPER fields.
use crate::native_trading_config::NativeTradingConfig;
use serde_json::{json,Value};
use std::collections::{BTreeMap,BTreeSet};

const SNAP_SHA:&str="2a44a10d6812dc21a1b80772bdbe0e2a8b8d7e1c1b47ecc918b0a01275469fc9";
const R166_ORIGIN_SHA:&str="7e6a4dcdfdf146359971f0eb17e82434dddbcc0c47b5af605c2e6c612ab075dd";
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum R176Error{Schema,Source,Historical,Overlay,UnsafeClaim}
fn sha(bytes:&[u8])->String{
 openssl::sha::sha256(bytes).iter().map(|x|format!("{x:02x}")).collect()
}
fn u(v:&Value,key:&str)->Option<u64>{v.get(key)?.as_u64()}
fn fields(v:&Value,key:&str)->Result<Vec<String>,R176Error>{
 let entries=v.get(key).and_then(Value::as_array).ok_or(R176Error::Overlay)?;
 entries.iter().map(|x|x.get("field").and_then(Value::as_str)
  .map(str::to_string).ok_or(R176Error::Overlay)).collect()
}
/// This is a SOURCE-ONLY crosswalk. It intentionally has no way to set
/// installed_paper_applied=true, and rejects a caller claiming that it did.
pub fn inspect_r176(
 yaml:&str,snapshot_text:&str,schema_text:&str,
 overlays:[&str;4],module_sources:[&str;4],claimed_installed:bool,
)->Result<Value,R176Error>{
 if claimed_installed{return Err(R176Error::UnsafeClaim)}
 let expected_yaml=include_str!("../config/native_trading_operator_baseline_r64.yaml");
 if sha(yaml.as_bytes())!=sha(expected_yaml.as_bytes()){
  return Err(R176Error::Source)
 }
 let cfg=NativeTradingConfig::parse(yaml).map_err(|_|R176Error::Schema)?;
 if cfg.mirrored_leaf_paths!=156 || cfg.mirrored_sections!=26
  || !cfg.source_projection_unchanged(){return Err(R176Error::Schema)}
 let schema:BTreeMap<String,String>=serde_json::from_str(schema_text).map_err(|_|R176Error::Schema)?;
 if schema.len()!=160{return Err(R176Error::Schema)}
 if sha(snapshot_text.as_bytes())!=SNAP_SHA{return Err(R176Error::Historical)}
 let prior:Value=serde_json::from_str(snapshot_text).map_err(|_|R176Error::Historical)?;
 if prior["schema"]!="R176_ORIGINAL_R166_HISTORICAL_SOURCE_SNAPSHOT_V1"
  ||prior["origin_sha256"]!=R166_ORIGIN_SHA
  ||u(&prior,"origin_reported_research")!=Some(123)
  ||u(&prior,"installed_paper_proven")!=Some(0){
  return Err(R176Error::Historical)
 }
 let rows=prior["rows"].as_array().ok_or(R176Error::Historical)?;
 if rows.len()!=160{return Err(R176Error::Historical)}
 let mut statuses=BTreeMap::<String,String>::new();
 for row in rows{
  let name=row["field"].as_str().ok_or(R176Error::Historical)?;
  let typ=row["type"].as_str().ok_or(R176Error::Historical)?;
  let evidence=row["research"].as_str().ok_or(R176Error::Historical)?;
  if schema.get(name).map(String::as_str)!=Some(typ)
   || !matches!(evidence,"TESTED_RESEARCH_READONLY"|"NOT_EVIDENCED")
   ||statuses.insert(name.into(),evidence.into()).is_some(){
   return Err(R176Error::Historical)
  }
 }
 if statuses.values().filter(|v|v.as_str()=="TESTED_RESEARCH_READONLY").count()!=123{
  return Err(R176Error::Historical)
 }
 let parsed=overlays.map(|raw|serde_json::from_str::<Value>(raw).map_err(|_|R176Error::Overlay));
 let [a,b,c,d]=parsed;
 let (a,b,c,d)=(a?,b?,c?,d?);
 let expected=[
  ("src/native_r168_strategy_source_review.rs","source_suite_sha256"),
  ("src/native_r169_source_metadata_review.rs","source_suite_sha256"),
  ("src/native_r173_config_operational_preview.rs","source_suite_sha256"),
  ("src/native_r174_unwired_legacy_guard.rs","r174_source_sha256"),
 ];
 for (i,v) in [&a,&b,&c,&d].iter().enumerate(){
  if v[expected[i].1]!=sha(module_sources[i].as_bytes())
   || (i<3 && v["source_suite"]!=expected[i].0)
   || (i==3 && v["r174_source"]!=expected[i].0){
   return Err(R176Error::Overlay)
  }
 }
 if u(&a,"parent_research_evidenced")!=Some(123)
  ||u(&a,"new_distinct_research_evidenced")!=Some(16)
  ||u(&a,"cumulative_research_evidenced")!=Some(139)
  ||u(&b,"parent_research_evidenced")!=Some(139)
  ||u(&b,"new_distinct_research_evidenced")!=Some(5)
  ||u(&b,"cumulative_research_evidenced")!=Some(144)
  ||u(&c,"parent_research_evidenced")!=Some(144)
  ||u(&c,"new_distinct_research_evidenced")!=Some(5)
  ||u(&c,"cumulative_research_evidenced")!=Some(149)
  ||u(&d,"cumulative_research_evidenced")!=Some(149)
  ||u(&d,"research_not_evidenced")!=Some(11)
  ||u(&d,"new_distinct_research_evidenced")!=Some(0)
  ||u(&a,"installed_paper_source_fields_proven")!=Some(0)
  ||u(&b,"installed_paper_yaml_applications_proven")!=Some(0)
  ||u(&c,"installed_paper_yaml_applications_proven")!=Some(0)
  ||u(&d,"installed_paper_yaml_applications_proven")!=Some(0)
  ||a["real_execution_authorized"]!=false
  ||b["real_execution_authorized"]!=false
  ||c["real_execution_authorized"]!=false{
  return Err(R176Error::Overlay)
 }
 let mut added=BTreeMap::<String,&str>::new();
 for (stage,v,count) in [("R168",&a,16),("R169",&b,5),("R173",&c,5)]{
  let names=fields(v,"new_research_fields")?;
  if names.len()!=count{return Err(R176Error::Overlay)}
  for name in names{
   if statuses.get(&name).map(String::as_str)!=Some("NOT_EVIDENCED")
    ||added.insert(name,stage).is_some(){return Err(R176Error::Overlay)}
  }
 }
 if added.len()!=26{return Err(R176Error::Overlay)}
 let unsupported=d["tested_fields"].as_array().ok_or(R176Error::Overlay)?;
 if unsupported.len()!=11{return Err(R176Error::Overlay)}
 let mut not_wired=BTreeSet::new();
 for field in unsupported{
  let name=field.as_str().ok_or(R176Error::Overlay)?;
  if statuses.get(name).map(String::as_str)!=Some("NOT_EVIDENCED")
    ||added.contains_key(name)||!not_wired.insert(name.to_string()){
   return Err(R176Error::Overlay)
  }
 }
 let all_missing:BTreeSet<String>=statuses.iter()
  .filter(|(_,state)|state.as_str()=="NOT_EVIDENCED")
  .filter(|(name,_)|!added.contains_key(*name))
  .map(|(name,_)|name.clone()).collect();
 if all_missing!=not_wired{return Err(R176Error::Overlay)}
 let mut result=Vec::with_capacity(160);
 for row in rows{
  let name=row["field"].as_str().ok_or(R176Error::Historical)?;
  let historical=statuses.get(name).ok_or(R176Error::Historical)?;
  let stage=if historical=="TESTED_RESEARCH_READONLY"{"R166_HISTORICAL"}
    else{added.get(name).copied().unwrap_or("UNWIRED_R174_GUARD_ONLY")};
  result.push(json!({
   "field":name,"type":row["type"],"research_stage":stage,
   "research_sources":row["prior_sources"],
   "installed_paper_applied":"NOT_PROVEN",
   "actual_consumer_runtime":"NOT_ATTESTED",
  }));
 }
 Ok(json!({
  "schema":"ASTRA_R176_SOURCE_RUNTIME_MANIFEST_RESEARCH_V1",
  "operator_yaml_sha256":sha(yaml.as_bytes()),
  "r166_origin_sha256":R166_ORIGIN_SHA,
  "r166_historical_snapshot_sha256":SNAP_SHA,
  "field_count":160,"r166_historical":123,"r168":16,"r169":5,
  "r173":5,"r174_unsupported":11,"total_research_reported":149,
  "installed_paper_applied_proven":0,
  "runtime_elf_attested":false,"deployed_binary_changed":false,
  "real_trading_authorized":false,"source_only":true,
  "fields":result,
 }))
}
pub fn embedded_r176()->Result<Value,R176Error>{
 inspect_r176(
  include_str!("../config/native_trading_operator_baseline_r64.yaml"),
  include_str!("../config/native_field_r176_historical_160_snapshot.json"),
  include_str!("../config/native_field_schema.json"),
  [
   include_str!("../config/native_field_evidence_r168_strategy_source_review.json"),
   include_str!("../config/native_field_evidence_r169_source_metadata_review.json"),
   include_str!("../config/native_field_evidence_r173_operational_preview.json"),
   include_str!("../config/native_field_evidence_r174_unwired_guard.json"),
  ],
  [
   include_str!("native_r168_strategy_source_review.rs"),
   include_str!("native_r169_source_metadata_review.rs"),
   include_str!("native_r173_config_operational_preview.rs"),
   include_str!("native_r174_unwired_legacy_guard.rs"),
  ],false)
}
#[cfg(test)]
mod tests{
 use super::*;
 const Y:&str=include_str!("../config/native_trading_operator_baseline_r64.yaml");
 const S:&str=include_str!("../config/native_field_r176_historical_160_snapshot.json");
 const SC:&str=include_str!("../config/native_field_schema.json");
 fn overlays()->[&'static str;4]{[
  include_str!("../config/native_field_evidence_r168_strategy_source_review.json"),
  include_str!("../config/native_field_evidence_r169_source_metadata_review.json"),
  include_str!("../config/native_field_evidence_r173_operational_preview.json"),
  include_str!("../config/native_field_evidence_r174_unwired_guard.json"),
 ]}
 fn sources()->[&'static str;4]{[
  include_str!("native_r168_strategy_source_review.rs"),
  include_str!("native_r169_source_metadata_review.rs"),
  include_str!("native_r173_config_operational_preview.rs"),
  include_str!("native_r174_unwired_legacy_guard.rs"),
 ]}
 fn check(y:&str,s:&str,sc:&str,a:[&str;4],src:[&str;4],installed:bool)
  ->Result<Value,R176Error>{inspect_r176(y,s,sc,a,src,installed)}
 #[test]fn r176_embedded_160_crosswalk_exact_149_plus_11(){
  let v=embedded_r176().unwrap();
  assert_eq!(v["field_count"],160);assert_eq!(v["total_research_reported"],149);
  assert_eq!(v["r174_unsupported"],11);assert_eq!(v["installed_paper_applied_proven"],0);
  assert_eq!(v["fields"].as_array().unwrap().len(),160);
  assert!(v["fields"].as_array().unwrap().iter().all(|f|f["installed_paper_applied"]=="NOT_PROVEN"));
 }
 #[test]fn r176_refuses_installed_claim(){assert_eq!(
  check(Y,S,SC,overlays(),sources(),true),Err(R176Error::UnsafeClaim));}
 #[test]fn r176_modified_yaml_same_schema_is_not_original_source(){
  let changed=Y.replace("paper_max_trades_per_day: 3000","paper_max_trades_per_day: 2500");
  assert_ne!(changed,Y);
  assert_eq!(check(&changed,S,SC,overlays(),sources(),false),Err(R176Error::Source));
 }
 #[test]fn r176_mutated_historical_receipt_is_not_authoritative(){
  let changed=S.replacen("TESTED_RESEARCH_READONLY","NOT_EVIDENCED",1);
  assert_ne!(changed,S);
  assert_eq!(check(Y,&changed,SC,overlays(),sources(),false),Err(R176Error::Historical));
 }
 #[test]fn r176_missing_original_schema_field_fails(){
  let changed=SC.replacen("adaptive_entry.enabled","adaptive_entry.unknown",1);
  assert_ne!(changed,SC);
  assert_eq!(check(Y,S,&changed,overlays(),sources(),false),Err(R176Error::Historical));
 }
 #[test]fn r176_stale_source_suite_hash_fails(){
  let mut src=sources();src[0]="rust source changed";
  assert_eq!(check(Y,S,SC,overlays(),src,false),Err(R176Error::Overlay));
 }
 #[test]fn r176_fake_installed_evidence_denied(){
  let mut a=overlays(); let changed=a[2].replace(
   "\"installed_paper_yaml_applications_proven\": 0",
   "\"installed_paper_yaml_applications_proven\": 149");
  assert_ne!(changed,a[2]);a[2]=&changed;
  assert_eq!(check(Y,S,SC,a,sources(),false),Err(R176Error::Overlay));
 }
 #[test]fn r176_unsupported_11_cannot_be_counted_as_research(){
  let mut a=overlays();
  let changed=a[3].replace("\"research_not_evidenced\": 11","\"research_not_evidenced\": 0");
  assert_ne!(changed,a[3]);a[3]=&changed;
  assert_eq!(check(Y,S,SC,a,sources(),false),Err(R176Error::Overlay));
 }
 #[test]fn r176_build_fingerprint_includes_json_and_detects_mutated_bytes(){
  use std::{fs,path::{Path,PathBuf}};
  fn walk_rs(root:&Path,paths:&mut Vec<PathBuf>){
   for e in fs::read_dir(root).unwrap(){
    let p=e.unwrap().path();
    if p.is_dir(){walk_rs(&p,paths)}
    else if p.extension().and_then(|x|x.to_str())==Some("rs"){paths.push(p)}
   }
  }
  let root=PathBuf::from(env!("CARGO_MANIFEST_DIR"));
  let mut inputs=vec![
   root.join("Cargo.toml"),root.join("Cargo.lock"),root.join("build.rs"),
   root.join("offline_review_trade2f3b6/Cargo.toml")
  ];
  for e in fs::read_dir(root.join("config")).unwrap(){
   let p=e.unwrap().path();
   if p.is_file()&&matches!(p.extension().and_then(|x|x.to_str()),Some("json"|"yaml")){
    inputs.push(p);
   }
  }
  walk_rs(&root.join("src"),&mut inputs);
  walk_rs(&root.join("offline_review_trade2f3b6"),&mut inputs);
  inputs.sort();
  let evidence=root.join("config/native_field_r176_historical_160_snapshot.json");
  assert!(inputs.contains(&evidence));
  let fingerprint=|tamper:bool|{
   let mut h=openssl::sha::Sha256::new();
   for path in &inputs{
    let relative=path.strip_prefix(&root).unwrap().to_str().unwrap();
    let mut bytes=fs::read(path).unwrap();
    if tamper&&path==&evidence{bytes[0]^=1;}
    h.update(&(relative.len() as u64).to_be_bytes());
    h.update(relative.as_bytes());
    h.update(&(bytes.len() as u64).to_be_bytes());
    h.update(&bytes);
   }
   h.finish().iter().map(|b|format!("{b:02x}")).collect::<String>()
  };
  assert_eq!(fingerprint(false),env!("SPOTARB_R157_SOURCE_BUNDLE_SHA256"));
  assert_ne!(fingerprint(false),fingerprint(true));
 }
}
