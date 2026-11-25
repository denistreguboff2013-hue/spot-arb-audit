use serde_json::json;
use spotarb_native_market_core::authoritative_state::{
    legacy_source_sha256,verify_legacy_source_unchanged,NativeStateStore,
};
use std::{env,fs,path::Path};

fn main()->Result<(),String>{
    let args:Vec<String>=env::args().collect();
    if args.len()!=3{return Err("usage: r28_cutover_hash_barrier LEGACY_STATE NATIVE_STATE".into())}
    let source=Path::new(&args[1]);
    let native=Path::new(&args[2]);
    let raw=fs::read(source).map_err(|e|e.to_string())?;
    let store=NativeStateStore::open(native).map_err(|e|format!("open:{e:?}"))?;
    let state=store.load().map_err(|e|format!("load:{e:?}"))?
        .ok_or("native state missing")?;
    let hash_match=verify_legacy_source_unchanged(&raw,&state)
        .map_err(|e|format!("verify:{e:?}"))?;
    let current_hash=legacy_source_sha256(&raw);
    let expected_hash=state.legacy_source_sha256.clone();
    let out=json!({
        "r28_cutover_hash_barrier":hash_match,
        "source_hash_match":hash_match,
        "current_source_sha256":current_hash,
        "expected_source_sha256":expected_hash,
        "native_generation":state.generation,
        "native_active":state.active.is_some(),
        "python_writer_quiesce_still_required":true,
        "authority_switch_allowed":false,
        "reason":if hash_match{
            "hash barrier passes, but writer quiesce is still mandatory before cutover"
        }else{
            "legacy source changed after import; candidate is stale"
        }
    });
    println!("{}",serde_json::to_string_pretty(&out).map_err(|e|e.to_string())?);
    if hash_match{Ok(())}else{Err("R28 cutover hash barrier denied".into())}
}
