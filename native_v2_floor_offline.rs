//! Read-only operator transport for LOCAL owner floors. No external trust is implied.
use std::{env,fs,path::Path};
use spotarb_native_market_core::native_paper_v2_close_source_archive_review::{
    export_local_v2_floor_manifest,verify_local_v2_portable_floor_manifest,
    inspect_portable_v2_floor_successor,
    PortableOwnerFloorManifest,
};
fn usage()->&'static str{
    "usage: native_v2_floor_offline export ABS_OWNER_DIR\n       native_v2_floor_offline verify ABS_OWNER_DIR MANIFEST_JSON FILE_PIN_SHA256\n       native_v2_floor_offline check-next ABS_OWNER_DIR OLD_JSON OLD_PIN NEW_JSON NEW_PIN\nLOCAL ONLY: store manifest and its latest pin outside this VM; no exchange verification."
}
fn read_manifest(file:&str)->Result<PortableOwnerFloorManifest,String>{
    let path=Path::new(file);
    let meta=fs::symlink_metadata(path).map_err(|e|format!("manifest inaccessible: {e}"))?;
    if !meta.file_type().is_file()||meta.len()>2_000_000{
        return Err("manifest must be regular and <=2MB".into());
    }
    let bytes=fs::read(path).map_err(|e|format!("manifest read failed: {e}"))?;
    serde_json::from_slice(&bytes).map_err(|e|format!("manifest JSON rejected: {e}"))
}
fn run(args:&[String])->Result<(),String>{
    match args {
        [mode,dir] if mode=="export"=>{
            let value=export_local_v2_floor_manifest(Path::new(dir))
                .map_err(|e|format!("local export DENIED: {e:?}"))?;
            let out=serde_json::to_string_pretty(&value)
                .map_err(|e|format!("JSON export failed: {e}"))?;
            println!("{out}");
            eprintln!("LOCAL CLAIM ONLY; EXTERNAL CUSTODY NOT VERIFIED; PIN_SHA256={}",value.manifest_sha256);
            Ok(())
        }
        [mode,dir,file,pin] if mode=="verify"=>{
            let manifest=read_manifest(file)?;
            let local=verify_local_v2_portable_floor_manifest(
                Path::new(dir),&manifest,pin,
            ).map_err(|e|format!("local floor check DENIED: {e:?}"))?;
            println!("LOCAL_FLOOR_MATCH only owners={}; exchange_auth=false real_trading=false external_custody_unproven",
                local.examined_owner_files);
            Ok(())
        }
        [mode,dir,old_file,old_pin,new_file,new_pin] if mode=="check-next"=>{
            let previous=read_manifest(old_file)?;
            let candidate=read_manifest(new_file)?;
            let local=inspect_portable_v2_floor_successor(
                Path::new(dir),&previous,old_pin,&candidate,new_pin,
            ).map_err(|e|format!("successor DENIED: {e:?}"))?;
            println!("LOCAL_SUCCESSOR_PASS owners={}; no external custody proven; NO PIN ACCEPTED OR WRITTEN",
                local.examined_owner_files);
            Ok(())
        }
        _=>Err(usage().into()),
    }
}
fn main(){
    let args:Vec<_>=env::args().skip(1).collect();
    if let Err(e)=run(&args){
        eprintln!("{e}");
        std::process::exit(2);
    }
}
#[cfg(test)]
mod tests{
 use super::*;
 #[test]
 fn malformed_or_absent_operator_inputs_deny(){
   assert!(run(&[]).is_err());
   assert!(run(&["verify".into(),".".into(),"x".into(),"".into()]).is_err());
   assert!(run(&["export".into(),"/definitely/no/owner/dir".into()]).is_err());
 }
}
