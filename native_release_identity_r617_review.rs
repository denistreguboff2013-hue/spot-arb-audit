//! Astra A7 / R617-R626: strict LOCAL release evidence comparison.
//! No service, DB, credentials, network, exchange, or deployment access.
//! All inputs are untrusted; even matching inputs are NOT independent attestation.
use serde_json::{json,Value};
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ReleaseIdentityError {
 Missing,Malformed,DirtySource,SourceMismatch,ElfMismatch,
 Unobserved,WrongProfile,UnsupportedAuthority,
}
fn sha(v:&Value)->Option<&str>{
 let s=v.as_str()?;
 if s.len()==64&&s.bytes().all(|b|b.is_ascii_digit()||(b'a'..=b'f').contains(&b)){
  Some(s)
 }else{None}
}
/// Compare candidate build/provenance and an explicit read-only PROCESS report.
/// Refuse release-quality claims if any identity is missing or if the git
/// snapshot was dirty. The result is a LOCAL comparison, not an authorization.
pub fn inspect_local_paper_release_match(
 candidate:&Value,effective:&Value,observed:Option<&Value>
)->Result<Value,ReleaseIdentityError>{
 use ReleaseIdentityError as E;
 if candidate["schema"]!="SPOTARB/NATIVE/R157/CANDIDATE-BUILD-PROOF/V1"{
  return Err(E::Malformed)
 }
 let build=&candidate["build_snapshot"];
 let elf=&candidate["current_process"];
 let head=sha(&build["git_head_at_compile"]).ok_or(E::Missing)?;
 let status=sha(&build["git_porcelain_sha256_at_compile"]).ok_or(E::Missing)?;
 let bundle=sha(&build["first_party_source_bundle_sha256"]).ok_or(E::Missing)?;
 let lock=sha(&build["lockfile_sha256"]).ok_or(E::Missing)?;
 let yaml=sha(&build["operator_yaml_sha256"]).ok_or(E::Missing)?;
 let elf_hash=sha(&elf["elf_sha256"]).ok_or(E::Missing)?;
 if build["git_dirty_at_compile"]!=false{return Err(E::DirtySource)}
 if !build["rustc_version"].as_str().is_some_and(|x|x.starts_with("rustc "))
   || !build["target_triple"].as_str().is_some_and(|x|!x.is_empty()&&x!="UNKNOWN")
   || !build["profile"].as_str().is_some_and(|x|!x.is_empty()&&x!="UNKNOWN")
   || !elf["elf_bytes"].as_u64().is_some_and(|v|v>100_000)
   || elf["source"]!="/proc/self/exe"{
  return Err(E::Malformed)
 }
 if effective["profile"]!="source-paper-review-v1"{
  return Err(E::WrongProfile)
 }
 if sha(&effective["source_sha256"])!=Some(yaml)
   || effective["installed_runtime_source_bound"]!=false{
  return Err(E::SourceMismatch)
 }
 let observed=observed.ok_or(E::Unobserved)?;
 if observed["evidence_schema"]!="SPOTARB/PAPER/R617/OBSERVED-PROCESS/V1"
   || observed["evidence_source"]!="/proc/<pid>/exe"
   || observed["pid"].as_u64().map_or(true,|v|v==0)
   || observed["exchange_authenticated"]!=false
   || observed["real_trading_authorized"]!=false{
   return Err(E::UnsupportedAuthority)
 }
 if sha(&observed["process_elf_sha256"])!=Some(elf_hash){
   return Err(E::ElfMismatch)
 }
 if sha(&observed["config_sha256"])!=Some(yaml)
   || observed["profile"]!=effective["profile"]{
   return Err(E::SourceMismatch)
 }
 Ok(json!({
  "schema":"SPOTARB/PAPER/R617/LOCAL-RELEASE-IDENTITY-REVIEW/V1",
  "candidate_git_head":head,
  "candidate_git_porcelain_sha256":status,
  "first_party_source_bundle_sha256":bundle,
  "lockfile_sha256":lock,
  "operator_yaml_sha256":yaml,
  "compared_process_elf_sha256":elf_hash,
  "local_elf_and_config_fields_match":true,
  "local_match_is_exchange_attestation":false,
  "release_authorized":false,
  "installed_runtime_config_certified":false,
  "independent_build_or_operator_attestation":false,
  "real_trading_authorized":false
 }))
}
#[cfg(test)]
mod tests{
 use super::*;
 fn h(c:char)->String{std::iter::repeat(c).take(64).collect()}
 fn inputs()->(Value,Value,Value){
  let candidate=json!({
   "schema":"SPOTARB/NATIVE/R157/CANDIDATE-BUILD-PROOF/V1",
   "build_snapshot":{
    "git_head_at_compile":h('a'),"git_dirty_at_compile":false,
    "git_porcelain_sha256_at_compile":h('b'),
    "first_party_source_bundle_sha256":h('c'),"lockfile_sha256":h('d'),
    "operator_yaml_sha256":h('e'),"rustc_version":"rustc 1.86.0",
    "target_triple":"x86_64-unknown-linux-gnu","profile":"debug"
   },
   "current_process":{"source":"/proc/self/exe",
     "elf_sha256":h('f'),"elf_bytes":123456}
  });
  let effective=json!({"profile":"source-paper-review-v1",
    "source_sha256":h('e'),"installed_runtime_source_bound":false});
  let observed=json!({
   "evidence_schema":"SPOTARB/PAPER/R617/OBSERVED-PROCESS/V1",
   "evidence_source":"/proc/<pid>/exe","pid":1234,
   "process_elf_sha256":h('f'),"config_sha256":h('e'),
   "profile":"source-paper-review-v1",
   "exchange_authenticated":false,"real_trading_authorized":false
  });
  (candidate,effective,observed)
 }
 #[test]fn r617_matching_local_inputs_still_deny_release(){
  let (c,e,o)=inputs();
  let result=inspect_local_paper_release_match(&c,&e,Some(&o)).unwrap();
  assert_eq!(result["local_elf_and_config_fields_match"],true);
  for name in ["release_authorized","installed_runtime_config_certified",
               "independent_build_or_operator_attestation","real_trading_authorized"]{
    assert_eq!(result[name],false,"{}",name);
  }
 }
 #[test]fn r618_missing_process_refuses(){
  let (c,e,_)=inputs();
  assert_eq!(inspect_local_paper_release_match(&c,&e,None),
    Err(ReleaseIdentityError::Unobserved));
 }
 #[test]fn r619_dirty_worktree_refuses_even_if_hashes_match(){
  let (mut c,e,o)=inputs();c["build_snapshot"]["git_dirty_at_compile"]=json!(true);
  assert_eq!(inspect_local_paper_release_match(&c,&e,Some(&o)),
    Err(ReleaseIdentityError::DirtySource));
 }
 #[test]fn r620_invalid_commit_id_refuses(){
  let (mut c,e,o)=inputs();c["build_snapshot"]["git_head_at_compile"]=json!("unknown");
  assert_eq!(inspect_local_paper_release_match(&c,&e,Some(&o)),
    Err(ReleaseIdentityError::Missing));
 }
 #[test]fn r621_missing_dirty_status_fingerprint_refuses(){
  let (mut c,e,o)=inputs();
  c["build_snapshot"].as_object_mut().unwrap().remove("git_porcelain_sha256_at_compile");
  assert_eq!(inspect_local_paper_release_match(&c,&e,Some(&o)),
    Err(ReleaseIdentityError::Missing));
 }
 #[test]fn r622_wrong_running_elf_refuses(){
  let (c,e,mut o)=inputs();o["process_elf_sha256"]=json!(h('a'));
  assert_eq!(inspect_local_paper_release_match(&c,&e,Some(&o)),
    Err(ReleaseIdentityError::ElfMismatch));
 }
 #[test]fn r623_wrong_applied_source_refuses(){
  let (c,mut e,o)=inputs();e["source_sha256"]=json!(h('a'));
  assert_eq!(inspect_local_paper_release_match(&c,&e,Some(&o)),
    Err(ReleaseIdentityError::SourceMismatch));
 }
 #[test]fn r624_observation_claiming_real_authority_refuses(){
  let (c,e,mut o)=inputs();o["real_trading_authorized"]=json!(true);
  assert_eq!(inspect_local_paper_release_match(&c,&e,Some(&o)),
    Err(ReleaseIdentityError::UnsupportedAuthority));
 }
 #[test]fn r625_fallback_or_unknown_research_profile_refuses(){
  let (c,mut e,o)=inputs();e["profile"]=json!("COMPILED_FALLBACK");
  assert_eq!(inspect_local_paper_release_match(&c,&e,Some(&o)),
    Err(ReleaseIdentityError::WrongProfile));
 }
 #[test]fn r626_observed_profile_source_conflict_refuses(){
  let (c,e,mut o)=inputs();o["config_sha256"]=json!(h('a'));
  assert_eq!(inspect_local_paper_release_match(&c,&e,Some(&o)),
    Err(ReleaseIdentityError::SourceMismatch));
 }
}
