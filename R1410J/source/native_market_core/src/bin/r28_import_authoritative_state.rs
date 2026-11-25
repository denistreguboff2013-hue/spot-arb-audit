use serde_json::json;
use spotarb_native_market_core::authoritative_state::{
    import_legacy_python_state,NativeStateStore,PositionProvenance,
};
use std::{env,fs,path::Path};

fn main()->Result<(),String>{
    let args:Vec<String>=env::args().collect();
    if args.len()!=3{return Err("usage: r28_import_authoritative_state LEGACY_STATE DEST_NATIVE_STATE".into())}
    let source=Path::new(&args[1]);
    let dest=Path::new(&args[2]);
    if source==dest{return Err("source and destination must differ".into())}
    let raw=fs::read(source).map_err(|e|e.to_string())?;
    let imported=import_legacy_python_state(&raw).map_err(|e|format!("import:{e:?}"))?;
    let source_hash_before=imported.legacy_source_sha256.clone().ok_or("legacy source hash missing")?;
    let store=NativeStateStore::open(dest).map_err(|e|format!("open:{e:?}"))?;
    store.create(&imported).map_err(|e|format!("create:{e:?}"))?;
    let raw_after=fs::read(source).map_err(|e|e.to_string())?;
    let source_hash_after={
        use openssl::sha::sha256;
        let d=sha256(&raw_after);
        d.iter().map(|b|format!("{b:02x}")).collect::<String>()
    };
    if source_hash_after!=source_hash_before{
        let _=fs::remove_file(dest);
        return Err("legacy source changed during import; native candidate removed".into())
    }
    let reopened=store.load().map_err(|e|format!("reopen:{e:?}"))?
        .ok_or("native state missing after create")?;
    if reopened!=imported{return Err("reopen mismatch".into())}
    let (active,provenance,reconciliation_required)=match &reopened.active{
        Some(p)=>(true,Some(match p.provenance{
            PositionProvenance::Native=>"Native",
            PositionProvenance::LegacyPythonImport=>"LegacyPythonImport",
        }),Some(p.reconciliation_required)),
        None=>(false,None,None),
    };
    println!("{}",serde_json::to_string_pretty(&json!({
        "r28_legacy_import":true,
        "source_stable_during_import":true,
        "cutover_requires_python_writer_quiesce":true,
        "native_state_created":true,
        "reopen_exact":true,
        "schema_version":reopened.schema_version,
        "generation":reopened.generation,
        "active":active,
        "provenance":provenance,
        "reconciliation_required":reconciliation_required,
        "legacy_source_sha256":reopened.legacy_source_sha256,
        "legacy_closed_positions_ignored":reopened.legacy_closed_positions_ignored
    })).map_err(|e|e.to_string())?);
    Ok(())
}
