use serde_json::json;
use spotarb_native_market_core::{
    ingress_runtime_r1205::{run_r1246_volatile,Config},
    market_data_ingress_r1205::{Authority,Output},
};
use std::{collections::HashMap,fs,path::PathBuf,sync::{Arc,RwLock},time::Duration};
use tokio::sync::mpsc;

fn main_err(msg: impl std::fmt::Display) -> String { msg.to_string() }

#[tokio::main(flavor="multi_thread",worker_threads=4)]
async fn main() -> Result<(),String> {
    let args=std::env::args().skip(1).collect::<Vec<_>>();
    if args.len()!=5 { return Err("usage: r1248_topology_probe SECONDS COPIES GROUP_SIZE COHORT_FILE OUTPUT_JSON".into()); }
    let seconds=args[0].parse::<u64>().map_err(main_err)?;
    let copies=args[1].parse::<usize>().map_err(main_err)?;
    let group=args[2].parse::<usize>().map_err(main_err)?;
    if !(30..=300).contains(&seconds) || ![1,2].contains(&copies) || ![10,20,40].contains(&group) {
        return Err("R1248 bounds: duration 30..300s, copies 1|2, group size 10|20|40".into());
    }
    let out_path=PathBuf::from(&args[4]).canonicalize_parent().map_err(main_err)?;
    if !out_path.to_string_lossy().starts_with("/mnt/spotarb_db_test/r1248_runs/") {
        return Err("output must stay under isolated R1248 runs".into());
    }
    if out_path.exists(){return Err("output already exists".into());}
    let kill_path="/home/tmpadmin/spotbot/data/real_kill_switch.json";
    let kill:serde_json::Value=serde_json::from_slice(&fs::read(kill_path).map_err(main_err)?).map_err(main_err)?;
    if kill["state"]!="ENGAGED"{return Err("REAL kill switch not ENGAGED".into());}
    let cohort=fs::read_to_string(&args[3]).map_err(main_err)?;
    let bases=cohort.lines().map(str::trim).filter(|s|!s.is_empty()).map(str::to_owned).collect::<Vec<_>>();
    if bases.len()!=40 || bases.iter().collect::<std::collections::HashSet<_>>().len()!=40 {
        return Err("cohort must contain exactly 40 unique bases".into());
    }
    std::env::set_var("R1248_BINANCE_GROUP_SIZE",group.to_string());
    let authority:Authority=Arc::new(RwLock::new(HashMap::new()));
    let (tx,mut rx)=mpsc::channel(256);
    let drain=tokio::spawn(async move {
        let mut counts=HashMap::<String,u64>::new();
        while let Some(item)=rx.recv().await {
            let key=match item {
                Output::Reset{venue,base,..}=>format!("reset:{venue:?}:{base}"),
                Output::Frame(_)=>"frame".into(),
                Output::Book(_,_)=>"book".into(),
                Output::Resubscribe{session,base,..}=>format!("resubscribe:{session}:{base}"),
                Output::Blocked{venue,base,..}=>format!("blocked:{venue:?}:{base}"),
            };
            *counts.entry(key).or_default()+=1;
        }
        counts
    });
    let start=std::time::SystemTime::now();
    let cfg=Config{
        shard:0,bases:bases.clone(),copies,duration:Duration::from_secs(seconds),
        controlled_close_after:None,endpoint_variant:0,probe_rotation_after:None
    };
    let summary=run_r1246_volatile(cfg,authority,tx).await?;
    let drained=drain.await.map_err(main_err)?;
    let result=json!({
        "schema":"SPOTARB/R1248/ISOLATED_PUBLIC_TOPOLOGY/V1",
        "strategy_context":"OKX Spot BUY + Binance USD-M Futures SHORT; public market-data diagnostics only",
        "started_system_time":format!("{start:?}"),
        "duration_requested_s":seconds,
        "binance_group_size":group,
        "binance_copies_per_group":copies,
        "binance_socket_count":(40+group-1)/group*copies,
        "okx_group_size":10,
        "okx_copies_per_group":copies,
        "okx_socket_count":4*copies,
        "bases":bases,
        "freshness_gate_ms":800,
        "kill_switch":"ENGAGED",
        "real_orders":false,"paper":false,"db1_db2":"unused","universum":"untouched",
        "drained_outputs":drained,
        "summary":summary
    });
    let bytes=serde_json::to_vec_pretty(&result).map_err(main_err)?;
    let mut f=std::fs::OpenOptions::new().write(true).create_new(true).open(&out_path).map_err(main_err)?;
    use std::io::Write;
    f.write_all(&bytes).map_err(main_err)?;
    f.sync_all().map_err(main_err)?;
    println!("{}",json!({"output":out_path,"duration_s":seconds,"copies":copies,"binance_group_size":group,"orders":0}));
    Ok(())
}

trait CanonicalizeParent { fn canonicalize_parent(&self)->std::io::Result<PathBuf>; }
impl CanonicalizeParent for PathBuf {
    fn canonicalize_parent(&self)->std::io::Result<PathBuf>{
        let parent=self.parent().ok_or_else(||std::io::Error::other("output parent missing"))?.canonicalize()?;
        Ok(parent.join(self.file_name().ok_or_else(||std::io::Error::other("output file name missing"))?))
    }
}
