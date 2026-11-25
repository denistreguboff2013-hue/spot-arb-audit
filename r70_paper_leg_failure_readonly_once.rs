//! R70: zero-network operator review of OFFLINE CRAFTED failure scenarios.
 //! Never uses market, private credentials, order transport or active PAPER state.
use spotarb_native_market_core::{
 native_trading_config::NativeTradingConfig,
 native_paper_leg_failure_review::{inspect_paper_leg_failure,LegFailurePhase},
};
use serde_json::{json,Value};
use std::path::PathBuf;
fn main()->Result<(),Box<dyn std::error::Error>>{
 if std::env::args().len()!=1{return Err("no flags: offline readonly only".into())}
 let yaml=PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/native_trading.yaml");
 let config=NativeTradingConfig::load(&yaml)
  .map_err(|e|format!("config rejected: {e:?}"))?;
 let scenarios=[
  ("spot_unknown_deadline",LegFailurePhase::SpotSubmitUnknown,4001,None),
  ("spot_partial",LegFailurePhase::SpotPartial,100,None),
  ("spot_full_short_delayed",LegFailurePhase::SpotConfirmedAwaitingHedge,1501,None),
  ("short_ack_unknown",LegFailurePhase::FuturesShortUnknown,1800,None),
  ("short_partial",LegFailurePhase::FuturesShortPartial,120,None),
  ("hedged_adverse_stop_review",LegFailurePhase::Hedged,100,Some(50.0)),
  ("cover_ack_unknown",LegFailurePhase::FuturesCoverUnknown,120,None),
  ("spot_sale_unknown",LegFailurePhase::SpotSaleUnknown,120,None),
  ("paper_closed_not_real",LegFailurePhase::PaperModeledClosed,120,None),
 ];
 let reports:Vec<Value>=scenarios.iter().map(|(label,phase,age,mark)|{
  inspect_paper_leg_failure(&config,*phase,*age,*mark)
   .map(|v|json!({"scenario":label,"review":v}))
   .map_err(|e|format!("{label}: {e:?}"))
 }).collect::<Result<_,_>>()?;
 println!("{}",json!({
  "schema":"r70_offline_readonly_failure_scenarios_v1",
  "source":"CRAFTED_NON_LIVE_NO_EXCHANGE_INPUT",
  "config_sha256":config.sha256,
  "cases":reports,
  "market_frames_replayed":0,
  "exchange_trades_attested":false,
  "order_transport_present":false,
  "telegram_sent":false,
  "running_paper_modified":false
 }));
 Ok(())
}
