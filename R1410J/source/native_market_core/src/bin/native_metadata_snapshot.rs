//! Emit one pure-native canonical metadata control snapshot. Public reads only.
use serde_json::json;
use spotarb_native_market_core::market_metadata::fetch_current_metadata;

fn main() {
    let generation = std::env::args()
        .nth(1)
        .and_then(|x| x.parse::<u64>().ok())
        .unwrap_or(1);
    let snapshot = match fetch_current_metadata(generation) {
        Ok(v) => v,
        Err(e) => {
            println!("{}", json!({"ok":false,"error":format!("{e:?}"),"trading":false}));
            std::process::exit(2);
        }
    };
    let mut value=snapshot.canonical_control_value();
    if let Some(obj)=value.as_object_mut() {
        obj.insert("ok".into(),json!(true));
        obj.insert("source".into(),json!("pure_native_https"));
        obj.insert("binance_active".into(),json!(snapshot.binance_active));
        obj.insert("binance_tradifi_excluded".into(),json!(snapshot.binance_tradifi_excluded));
        obj.insert("okx_active".into(),json!(snapshot.okx_active));
        obj.insert("intersection".into(),json!(snapshot.intersection()));
        obj.insert("metadata_ready".into(),json!(snapshot.metadata_ready));
        obj.insert("static_denied".into(),json!(snapshot.static_denied));
        obj.insert("market_metadata".into(),serde_json::to_value(&snapshot.markets).unwrap());
        obj.insert("loaded_ns".into(),json!(snapshot.loaded_ns));
        obj.insert("loaded_wall_ms".into(),json!(snapshot.loaded_wall_ms));
        obj.insert("trading".into(),json!(false));
    }
    println!("{value}");
}
