//! R157 local build + exact current ELF SHA256. This is an UNSIGNED,
//! self-reported source/binary correlation, not release attestation.
//! Never equate this evidence with installed PAPER or real-order permission.
use openssl::sha::Sha256;
use serde_json::{Value,json};
use std::{fs::File,io::{self,Read}};
const BUNDLE:&str=env!("SPOTARB_R157_SOURCE_BUNDLE_SHA256");
const LOCK:&str=env!("SPOTARB_R157_LOCK_SHA256");
const YAML:&str=env!("SPOTARB_R157_YAML_SHA256");
const GIT:&str=env!("SPOTARB_R157_GIT_HEAD");
const DIRTY:&str=env!("SPOTARB_R157_GIT_DIRTY");
const STATUS_SHA:&str=env!("SPOTARB_R617_GIT_STATUS_SHA256");
const RUSTC:&str=env!("SPOTARB_R157_RUSTC_VERSION");
const TARGET:&str=env!("SPOTARB_R157_TARGET");
const PROFILE:&str=env!("SPOTARB_R157_PROFILE");
const OPT:&str=env!("SPOTARB_R157_OPT_LEVEL");

fn hex(bytes:&[u8])->String{
 bytes.iter().map(|b|format!("{b:02x}")).collect()
}
fn valid_sha(s:&str)->bool{
 s.len()==64&&s.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase())
}
/// Hash the process's actual mapped Linux executable, not a path chosen by
/// the operator and not another file from target/debug or service symlink.
pub fn self_elf_sha256()->Result<(String,u64),io::Error>{
 let mut f=File::open("/proc/self/exe")?;
 let mut magic=[0_u8;4];f.read_exact(&mut magic)?;
 if magic!=*b"\x7fELF"{
  return Err(io::Error::new(io::ErrorKind::InvalidData,
   "current process image is not ELF"))
 }
 let mut sha=Sha256::new();
 sha.update(&magic);
 let mut count=4_u64;
 let mut b=[0_u8;1024*1024];
 loop{
  let n=f.read(&mut b)?;
  if n==0{break}
  count=count.checked_add(n as u64)
   .ok_or_else(||io::Error::new(io::ErrorKind::InvalidData,
     "self ELF length overflow"))?;
  sha.update(&b[..n]);
 }
 Ok((hex(&sha.finish()),count))
}
/// Build snapshot fields come from Cargo's build script. The SHA256 of
/// /proc/self/exe is obtained at runtime. Both remain untrusted absent
/// independent validation against the actually installed process.
pub fn candidate_build_provenance()->Result<Value,io::Error>{
 if ![BUNDLE,LOCK,YAML,STATUS_SHA].iter().all(|s|valid_sha(s)){
  return Err(io::Error::new(io::ErrorKind::InvalidData,
   "missing or invalid compiled source SHA256"))
 }
 let (elf_sha,elf_size)=self_elf_sha256()?;
 if !valid_sha(&elf_sha){
  return Err(io::Error::new(io::ErrorKind::InvalidData,
   "invalid executable SHA256"))
 }
 Ok(json!({
  "schema":"SPOTARB/NATIVE/R157/CANDIDATE-BUILD-PROOF/V1",
  "build_snapshot":{
   "git_head_at_compile":GIT,
   "git_dirty_at_compile":DIRTY=="true",
   "git_porcelain_sha256_at_compile":STATUS_SHA,
   "first_party_source_bundle_sha256":BUNDLE,
   "bundle_scope":"Cargo.toml Cargo.lock build.rs config/*.yaml config/*.json src/**/*.rs offline_review_trade2f3b6/Cargo.toml offline_review_trade2f3b6/**/*.rs",
   "lockfile_sha256":LOCK,
   "operator_yaml_sha256":YAML,
   "rustc_version":RUSTC,
   "target_triple":TARGET,
   "profile":PROFILE,
   "opt_level":OPT,
   "offline_b6_review_feature":cfg!(feature="offline-b6-review")
  },
  "current_process":{
   "source":"/proc/self/exe",
   "elf_sha256":elf_sha,
   "elf_bytes":elf_size
  },
  "limits":{
   "local_executable_hash_is_observed":true,
   "build_metadata_is_self_reported":true,
   "external_build_signature_verified":false,
   "installed_paper_pid_exe_compared":false,
   "deployed_elf_attested":false,
   "operator_source_applied_at_runtime":false,
   "public_ws_exchange_peer_attested":false,
   "real_trading_authorized":false
  }
 }))
}
#[cfg(test)]
mod tests{
 use super::*;
 #[test]
 fn r157_exact_running_test_elf_is_hashed_twice_and_has_valid_build_inputs(){
  let a=candidate_build_provenance().unwrap();
  let b=candidate_build_provenance().unwrap();
  let elf=a["current_process"]["elf_sha256"].as_str().unwrap();
  assert!(valid_sha(elf));
  assert_eq!(a["current_process"]["elf_sha256"],b["current_process"]["elf_sha256"]);
  assert!(a["current_process"]["elf_bytes"].as_u64().unwrap()>100_000);
  assert!(valid_sha(a["build_snapshot"]["first_party_source_bundle_sha256"].as_str().unwrap()));
  assert!(valid_sha(a["build_snapshot"]["operator_yaml_sha256"].as_str().unwrap()));
  assert!(valid_sha(a["build_snapshot"]["lockfile_sha256"].as_str().unwrap()));
  assert_eq!(a["limits"]["deployed_elf_attested"],false);
  assert_eq!(a["limits"]["installed_paper_pid_exe_compared"],false);
  assert_eq!(a["limits"]["real_trading_authorized"],false);
 }
 #[test]
 fn r157_invalid_digests_are_denied(){
  assert!(!valid_sha("unknown"));
  assert!(!valid_sha(&"G".repeat(64)));
  assert!(!valid_sha(&"0".repeat(63)));
  assert!(valid_sha(&"a".repeat(64)));
 }
}
