//! Pure-native current-universe gate. Public metadata only; trading=false.
use serde_json::json;
use spotarb_native_market_core::market_metadata::fetch_current_metadata;
use std::collections::BTreeMap;

fn main() {
    let snapshot = match fetch_current_metadata(1) {
        Ok(v) => v,
        Err(e) => {
            println!("{}", json!({"r8_universe_gate":false,"error":format!("{e:?}"),"trading":false}));
            std::process::exit(2);
        }
    };
    let mut reasons:BTreeMap<String,u64>=BTreeMap::new();
    for row in snapshot.markets.values() {
        if let Some(reason)=&row.static_reason { *reasons.entry(reason.clone()).or_default()+=1; }
    }
    let bases=snapshot.bases();
    println!("{}",json!({
        "r8_universe_gate": snapshot.intersection()>0 && snapshot.metadata_ready+snapshot.static_denied==snapshot.intersection(),
        "source":"pure_native_https",
        "binance_active":snapshot.binance_active,
        "binance_tradifi_excluded":snapshot.binance_tradifi_excluded,
        "okx_active":snapshot.okx_active,
        "intersection":snapshot.intersection(),
        "metadata_ready":snapshot.metadata_ready,
        "static_denied":snapshot.static_denied,
        "deny_reasons":reasons,
        "first":bases.first(),
        "last":bases.last(),
        "bases":bases,
        "generation":snapshot.generation,
        "loaded_ns":snapshot.loaded_ns,
        "loaded_wall_ms":snapshot.loaded_wall_ms,
        "trading":false
    }));
}
