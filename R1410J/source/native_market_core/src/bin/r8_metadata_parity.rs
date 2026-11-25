//! Pure-native live metadata vs retained Python/CCXT oracle. Public reads only.
use serde_json::{json,Value};
use spotarb_native_market_core::market_metadata::fetch_current_metadata;
use std::fs;

fn num_eq(a:Option<&str>, b:&Value)->bool {
    match (a,b.as_str()) {
        (None,None)=>true,
        (Some(x),Some(y))=>x.parse::<f64>().ok().zip(y.parse::<f64>().ok()).is_some_and(|(a,b)|a==b),
        _=>false
    }
}
fn main(){
    let oracle:Vec<Value>=serde_json::from_str(&fs::read_to_string("/mnt/spotarb_db_test/r8_python_oracle.json").unwrap()).unwrap();
    let snapshot=match fetch_current_metadata(1){
        Ok(v)=>v,
        Err(e)=>{println!("{}",json!({"r8_metadata_parity":false,"error":format!("{e:?}"),"trading":false}));std::process::exit(2);}
    };
    let mut mismatches=Vec::new();let mut missing=Vec::new();
    for q in &oracle {
        let base=q["base"].as_str().unwrap_or("");
        let Some(m)=snapshot.markets.get(base) else {missing.push(base.to_string());continue};
        let checks=[
            ("spot_min",m.spot_min_amount.as_deref()),
            ("spot_step",m.spot_step.as_deref()),
            ("spot_tick",m.spot_tick.as_deref()),
            ("future_min",m.future_min_base.as_deref()),
            ("future_step",m.future_step_base.as_deref()),
            ("future_max",m.future_max_base.as_deref()),
            ("future_tick",m.future_tick.as_deref()),
            ("future_min_cost",Some(m.future_min_cost.as_str())),
        ];
        let mut bad=Vec::new();
        for (key,value) in checks { if !num_eq(value,&q[key]) { bad.push(key); } }
        if q["spot_status"].as_str()!=Some(m.spot_status.as_str()){bad.push("spot_status");}
        if q["future_status"].as_str()!=Some(m.future_status.as_str()){bad.push("future_status");}
        if !bad.is_empty(){mismatches.push(json!({"base":base,"fields":bad}));}
    }
    println!("{}",json!({
        "r8_metadata_parity":mismatches.is_empty()&&missing.is_empty(),
        "source":"pure_native_https",
        "oracle_markets":oracle.len(),
        "current_intersection":snapshot.intersection(),
        "missing_since_oracle":missing.len(),
        "metadata_mismatches":mismatches.len(),
        "missing_bases":missing,
        "mismatch_samples":mismatches.into_iter().take(20).collect::<Vec<_>>(),
        "trading":false
    }));
}
