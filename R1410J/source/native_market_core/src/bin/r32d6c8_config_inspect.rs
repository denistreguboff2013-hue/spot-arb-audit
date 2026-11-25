//! R32D6C8A: READ-ONLY and credentials-free review config inspector.
//! Never connects to exchanges, performs trading or modifies any file.
use std::path::PathBuf;
use spotarb_native_market_core::native_trading_config::NativeTradingConfig;
fn main(){
 let args:Vec<_>=std::env::args_os().collect();
 if args.len()>2{
  eprintln!("Usage: r32d6c8_config_inspect [review-config.yaml]");
  std::process::exit(2);
 }
 let path=if let Some(v)=args.get(1){PathBuf::from(v)}else{
  PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/native_trading.yaml")
 };
 match NativeTradingConfig::load(&path){
  Ok(cfg)=>{
   let p=&cfg.policy;
   println!("R32D6C8A_CONFIG_VALIDATED_READ_ONLY");
   println!("status={}",cfg.migration_status);
   println!("sections={} original_leaf_keys={}",cfg.mirrored_sections,cfg.mirrored_leaf_paths);
   println!("config_sha256={}",cfg.sha256);
   println!("risk.futures_order_usdt={}",p.nominal_order_e18/1_000_000_000_000_000_000);
   println!("risk.max_open_positions={}",p.general_max_positions);
   println!("real_trial.max_open_positions={}",p.real_max_positions);
   println!("real_trial.max_trades_per_day={}",p.real_daily_trades);
   println!("limits.max_trades_per_day={}",p.general_daily_trades);
   println!("limits.paper_max_trades_per_day={}",p.paper_daily_trades);
   println!("real_trial.entry_cooldown_sec={}",p.real_cooldown_sec);
   println!("spread.entry_bps={:.3}",p.entry_min_milli_bps as f64/1000.0);
   println!("spread.reserve_bps={:.3}",p.fee_reserve_milli_bps as f64/1000.0);
   println!("real_execution_authorized=false");
   println!("may_send_order=false");
   println!("NOTE: mirrored parameters are NOT all wired into trading/analytics.");
  },
  Err(e)=>{
   eprintln!("R32D6C8A_REJECTED_CONFIG: {e:?}");
   std::process::exit(3);
  }
 }
}
