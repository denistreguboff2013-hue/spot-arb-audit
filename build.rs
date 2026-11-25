//! R157 first-party source bundle fingerprint at Cargo build time.
//! This is local reproducibility metadata, NOT signed attestation or a
//! statement about the binary installed by a supervisor.
use openssl::sha::Sha256;
use std::{env,fs,path::{Path,PathBuf},process::Command};

fn hex(bytes:&[u8])->String{
 bytes.iter().map(|b|format!("{b:02x}")).collect()
}
fn files_recursively(dir:&Path,paths:&mut Vec<PathBuf>){
 let entries=fs::read_dir(dir)
  .unwrap_or_else(|e|panic!("R157 cannot walk {}: {e}",dir.display()));
 for entry in entries{
  let p=entry.expect("R157 unreadable directory entry").path();
  if p.is_dir(){files_recursively(&p,paths)}
  else if p.extension().and_then(|x|x.to_str())==Some("rs"){
   paths.push(p)
  }
 }
}
fn hash_bytes(data:&[u8])->String{hex(&openssl::sha::sha256(data))}
/// R176: source-built evidence JSON is part of the executable contract.
/// Previously the bundle fingerprint silently omitted config/*.json.
fn config_inputs(dir:&Path,paths:&mut Vec<PathBuf>){
 for entry in fs::read_dir(dir).expect("R176 config directory"){
  let p=entry.expect("R176 config input").path();
  if p.is_file() && matches!(p.extension().and_then(|e|e.to_str()),Some("json"|"yaml")){
   paths.push(p);
  }
 }
}
fn hash_first_party(manifest:&Path)->String{
 let mut paths=vec![manifest.join("Cargo.toml"),manifest.join("Cargo.lock"),
  manifest.join("offline_review_trade2f3b6/Cargo.toml"),
  manifest.join("build.rs")];
 config_inputs(&manifest.join("config"),&mut paths);
 files_recursively(&manifest.join("src"),&mut paths);
 // Optional path-dependency sources are first-party and must be included.
 files_recursively(&manifest.join("offline_review_trade2f3b6"),&mut paths);
 paths.sort();
 let mut h=Sha256::new();
 for p in paths{
  let relative=p.strip_prefix(manifest).expect("within manifest");
  let name=relative.to_str().expect("UTF-8 first-party source");
  let bytes=fs::read(&p).unwrap_or_else(|e|panic!("R157 missing {name}: {e}"));
  h.update(&(name.len() as u64).to_be_bytes());
  h.update(name.as_bytes());
  h.update(&(bytes.len() as u64).to_be_bytes());
  h.update(&bytes);
 }
 hex(&h.finish())
}
fn git(root:&Path,args:&[&str])->Option<String>{
 let o=Command::new("git").arg("-C").arg(root).args(args).output().ok()?;
 if !o.status.success(){return None}
 String::from_utf8(o.stdout).ok().map(|s|s.trim().to_string())
}
fn main(){
 let manifest=PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest path"));
 let repo=manifest.parent().expect("repo path");
 println!("cargo:rerun-if-changed=src");
 println!("cargo:rerun-if-changed=offline_review_trade2f3b6");
 // JSON evidence and schema changes must invalidate the build provenance.
 println!("cargo:rerun-if-changed=config");
 println!("cargo:rerun-if-changed=Cargo.lock");
 println!("cargo:rerun-if-changed=Cargo.toml");
 println!("cargo:rerun-if-changed=build.rs");
 println!("cargo:rerun-if-changed=../.git/HEAD");
 // HEAD itself names a branch and does not change with new commits.
 // Watch the branch's resolved ref too, or Git HEAD would be stale.
 if let Some(branch)=git(repo,&["symbolic-ref","-q","HEAD"]){
  if let Some(path)=git(repo,&["rev-parse","--git-path",&branch]){
   let p=PathBuf::from(path);
   let target=if p.is_absolute(){p}else{repo.join(p)};
   println!("cargo:rerun-if-changed={}",target.display());
  }
 }
 // R965: watch every hashed first-party file individually. A directory
 // watch alone has allowed stale R157 embedded hashes in test binaries
 // after nested include! source changes; an exact per-file dependency
 // forces Cargo to re-run the provenance builder on content drift.
 let mut watched=Vec::new();
 config_inputs(&manifest.join("config"),&mut watched);
 files_recursively(&manifest.join("src"),&mut watched);
 files_recursively(&manifest.join("offline_review_trade2f3b6"),&mut watched);
 watched.extend([
  manifest.join("Cargo.toml"),manifest.join("Cargo.lock"),
  manifest.join("build.rs"),
  manifest.join("offline_review_trade2f3b6/Cargo.toml")
 ]);
 watched.sort();watched.dedup();
 for path in watched {
  println!("cargo:rerun-if-changed={}",path.display());
 }
 println!("cargo:rerun-if-env-changed=RUSTFLAGS");
 let src=hash_first_party(&manifest);
 let lock=hash_bytes(&fs::read(manifest.join("Cargo.lock")).expect("locked deps"));
 let yaml=hash_bytes(&fs::read(manifest.join("config/native_trading.yaml"))
  .expect("versioned operator source"));
 let head=git(repo,&["rev-parse","HEAD"]).unwrap_or_else(||"UNKNOWN".into());
 let porcelain=Command::new("git").arg("-C").arg(repo)
  .args(["status","--porcelain=v1","--untracked-files=normal"])
  .output().ok().filter(|o|o.status.success()).map(|o|o.stdout);
 let dirty=porcelain.as_ref().map_or(true,|o|!o.is_empty());
 let porcelain_sha=porcelain.as_ref().map(|o|hash_bytes(o))
  .unwrap_or_else(||"UNKNOWN".to_string());
 let rustc=env::var("RUSTC").unwrap_or_else(|_|"rustc".into());
 let rustc_version=Command::new(rustc).arg("--version").output().ok()
  .and_then(|o|if o.status.success(){String::from_utf8(o.stdout).ok()}else{None})
  .unwrap_or_else(||"UNKNOWN".into()).trim().to_string();
 for (key,val) in [
  ("SPOTARB_R157_SOURCE_BUNDLE_SHA256",src),
  ("SPOTARB_R157_LOCK_SHA256",lock),
  ("SPOTARB_R157_YAML_SHA256",yaml),
  ("SPOTARB_R157_GIT_HEAD",head),
  ("SPOTARB_R157_GIT_DIRTY",dirty.to_string()),
  ("SPOTARB_R617_GIT_STATUS_SHA256",porcelain_sha),
  ("SPOTARB_R157_RUSTC_VERSION",rustc_version),
  ("SPOTARB_R157_TARGET",env::var("TARGET").unwrap_or_else(|_|"UNKNOWN".into())),
  ("SPOTARB_R157_PROFILE",env::var("PROFILE").unwrap_or_else(|_|"UNKNOWN".into())),
  ("SPOTARB_R157_OPT_LEVEL",env::var("OPT_LEVEL").unwrap_or_else(|_|"UNKNOWN".into())),
 ]{println!("cargo:rustc-env={key}={val}")}
}
