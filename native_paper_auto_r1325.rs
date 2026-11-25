// Automatic public-data PAPER policy. One existing R1289 owner; no transport capability.
mod turnover {include!("native_paper_turnover_r1325.rs");}
pub(super) struct AutoPolicy { config:core::native_trading_config::NativeTradingConfig,turnover:Arc<turnover::Turnover> }
impl AutoPolicy {
 pub(super) fn load()->Result<Self,Failure>{
  let config=core::native_trading_config::NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).map_err(offline_adapter_error)?;
  let p=Self{config,turnover:Arc::new(turnover::Turnover::default())};let d=p.decision()?;
  if d.max_age_ms!=800{return Err("R1325_AUTO_FRESHNESS_MUST_BE_800_MS".into())}
  core::native_paper_config_bridge::bind_source_paper_quotas_review(&p.config).map_err(offline_adapter_error)?;
  core::native_paper_config_bridge::bind_public_paper_temporal_policy_review(&p.config).map_err(offline_adapter_error)?;
  core::native_paper_model_config_review::PaperModelConfigReview::from_config(&p.config).map_err(offline_adapter_error)?;
  Ok(p)
 }
 fn decision(&self)->Result<DecisionConfig,Failure>{Ok(core::native_paper_config_bridge::inspect_public_paper_policy_readonly(&self.config).map_err(offline_adapter_error)?)}
 fn number(&self,section:&str,key:&str)->Result<f64,Failure>{
  let n=self.config.raw.get(section).and_then(|s|s.get(key)).and_then(|v|v.as_f64()).ok_or("R1325_AUTO_MISSING_CONFIG_NUMBER")?;
  if !n.is_finite()||n<0.0{return Err("R1325_AUTO_INVALID_CONFIG_NUMBER".into())}Ok(n)
 }
 fn evidence(&self)->Value{json!({"mode":"AUTO_PAPER","config_sha256":self.config.sha256,"real_off":true,"kill_switch_required":"ENGAGED","profile_source":"current market_metadata; existing R1287 risk defaults; existing R1298 exit sequence","economics":"canonical aggregate round-trip reserve; no invented venue fee split","duration_ms":60000,"runtime_pass":false})}
 fn manifest(&self,meta:&core::market_metadata::NativeMetadataSnapshot,root:&std::path::Path)->Result<Manifest,Failure>{
  use core::fill_economics::{decimal_to_atoms as atoms,atoms_to_string as dec};
  let d=self.decision()?;let risk=paper_risk::RiskConfig::default();
  let run=root.file_name().and_then(|x|x.to_str()).ok_or("auto run id")?;
  let deadline=self.config.raw.get("exit_safety").and_then(|v|v.get("spot_chase")).and_then(|v|v.get("hard_deadline_ms")).and_then(|v|v.as_u64()).ok_or("R1325_AUTO_EXIT_DEADLINE_MISSING")?.checked_mul(1_000_000).ok_or("exit deadline overflow")?;
  let mut ids=Vec::new();
  for m in meta.markets.values().filter(|m|m.metadata_state()=="SIZE_METADATA_READY"){
   let a=atoms(m.spot_step.as_deref().ok_or("spot step")?).map_err(offline_adapter_error)?;
   let b=atoms(m.future_step_base.as_deref().ok_or("future step")?).map_err(offline_adapter_error)?;
   let step=auto_lcm(a,b)?;
   let lower=atoms(m.spot_min_amount.as_deref().ok_or("spot min")?).map_err(offline_adapter_error)?.max(atoms(m.future_min_base.as_deref().ok_or("future min")?).map_err(offline_adapter_error)?);
   let min=lower.checked_add(step-1).ok_or("min overflow")?/step*step;
   let budget_cap=atoms(&format!("{:.12}",d.risk_cap/d.min_price)).map_err(offline_adapter_error)?;
   let mut max=budget_cap.max(min);
   for v in [&m.spot_max_amount,&m.future_max_base].into_iter().flatten(){max=max.min(atoms(v).map_err(offline_adapter_error)?);}
   max=max/step*step;if min==0||max<min{return Err(format!("R1325_AUTO_INCONSISTENT_METADATA:{}",m.base).into())}
   let notional=atoms(&m.spot_min_cost).map_err(offline_adapter_error)?.max(atoms(&m.future_min_cost).map_err(offline_adapter_error)?);
   let profile=isolated_policy::InstrumentProfile{synthetic:true,base:m.base.clone(),spot_symbol:m.spot_symbol.clone(),futures_symbol:m.future_symbol.clone(),product:"LINEAR_USDT_PERPETUAL".into(),quantity_units:"BASE".into(),multiplier:"1".into(),hedge_ratio:"1".into(),step:dec(step),minimum_quantity:dec(min),maximum_quantity:dec(max),minimum_notional:dec(notional),fee_model:"QUOTE_SIMULATED".into(),fee_bps:risk.fee_bps.to_string(),maximum_unhedged_base:dec(max),maximum_dust_base:"0".into(),metadata_loaded_ns:meta.loaded_ns,metadata_ttl_ns:d.metadata_ttl_ms*1_000_000,hedge_timeout_ns:risk.hedge_timeout_ns,stop_loss_bps:risk.stop_loss_bps,max_hold_ns:risk.max_hold_ns};
   let id=isolated_policy::Identity{run_id:run.into(),clock_domain:"R1325_AUTO_MONOTONIC".into(),exit:Some(core::native_paper_exit_r1298::ExitRules{sequence:"SYNTHETIC_COVER_THEN_SPOT".into(),max_unhedged_base:profile.maximum_unhedged_base.clone(),max_exit_duration_ns:deadline,quantity_units:"BASE".into(),fee_model:"QUOTE_SIMULATED".into(),allow_next_generation:true}),profile};
   id.validate().map_err(offline_adapter_error)?;ids.push(id);
  }
  if ids.is_empty(){return Err("R1325_AUTO_EMPTY_ELIGIBLE_UNIVERSE".into())}
  let identity=ids.remove(0);Ok(Manifest{schema:1,runtime:"IntegratedPaper".into(),real_off:true,synthetic_policy_permitted:true,duration_ms:60000,identity,instruments:ids})
 }
}
fn auto_lcm(a:u128,b:u128)->Result<u128,Failure>{
 if a==0||b==0{return Err("R1325_AUTO_ZERO_STEP".into())}
 let(mut x,mut y)=(a,b);while y!=0{let r=x%y;x=y;y=r;}
 (a/x).checked_mul(b).ok_or_else(||"R1325_AUTO_STEP_OVERFLOW".into())
}
impl AutoPolicy {
 pub(super) fn check_turnover(&self,base:&str,n:u64,w:i64)->Result<Value,Failure>{Ok(self.turnover.witness(base,n,w,self.number("filters","min_24h_volume_usdt")?).map_err(offline_adapter_error)?)}
 pub(super) fn apply(&self,r:&mut IsolatedPaperRuntime,c:&isolated_policy::MarketContext,spot:&core::Book,futures:&core::Book,velocity:Option<f64>)->Result<bool,Failure>{
  let d=self.decision()?;
  if r.execution.lifecycle().state==paper_risk::LifecycleState::Flat{
   let quota=core::native_paper_config_bridge::bind_source_paper_quotas_review(&self.config).map_err(offline_adapter_error)?;
   let (entries,active,reserve,realized)=r.execution.auto_budget_r1325(d.cost).map_err(offline_adapter_error)?;
   let reason=if active>=quota.concurrent{"R1325_AUTO_CONCURRENT_LIMIT"}
    else if entries>=quota.daily{"R1325_AUTO_DAILY_LIMIT"}
    else if reserve+d.base_size*d.cost/10000.0>self.number("limits","max_okx_fees_per_day_usdt")?{"R1325_AUTO_FEE_RESERVE_LIMIT"}
    else if realized-reserve<=-self.number("limits","max_daily_loss_usdt")?{"R1325_AUTO_DAILY_LOSS_LIMIT"}
    else{""};
   if !reason.is_empty(){r.actions.push(json!({"action":"auto_entry_rejected","risk_result":reason}));return Ok(false)}
   match self.check_turnover(&r.identity.profile.base,c.now_ns,c.wall_ms){
    Ok(w)=>r.actions.push(json!({"action":"auto_turnover_admitted","witness":w})),
    Err(reason)=>{r.actions.push(json!({"action":"auto_entry_rejected","risk_result":reason.to_string()}));return Ok(false)}
   }

  }
  if r.execution.lifecycle().state==paper_risk::LifecycleState::Hedged&&!r.execution.exit_requested(){
   let l=r.execution.lifecycle().ledger.as_ref().ok_or("R1325_AUTO_LEDGER_MISSING")?;
   let observed=(futures.bids.first().ok_or("future bid")?.0/spot.asks.first().ok_or("spot ask")?.0-1.0)*10000.0;
   let close=(futures.asks.first().ok_or("future ask")?.0/spot.bids.first().ok_or("spot bid")?.0-1.0)*10000.0;
   let review=core::native_legacy_strategy_policy_review::inspect_legacy_close_readonly(&self.config,core::native_legacy_strategy_policy_review::LegacyCloseObservation{observed_spread_bps:observed,executable_close_spread_bps:Some(close),velocity_bps_s:velocity,elapsed_seconds:c.now_ns.saturating_sub(l.opened_ns)/1_000_000_000,position_units:isolated_policy::quantity_atoms(l.spot_exposure_base()).map_err(offline_adapter_error)?,force_close:false,futures_emergency_stop_hit:false}).map_err(offline_adapter_error)?;
   let projection=core::native_paper_execution_r1289::PortfolioProjection::derive(r.execution.lifecycle(),Some((spot,futures)));
   let net=projection.unrealized_pnl_quote.map(|v|v-l.spot_buy_quote*d.cost/10000.0);
   let target=l.spot_buy_quote*self.number("spread","net_target_bps")?/10000.0;
   use core::position_lifecycle::CloseDecision;
   if review.decision==CloseDecision::CloseEmergency||(review.decision==CloseDecision::CloseNormal&&net.is_some_and(|v|v>=target)){
    r.execution.request_isolated_exit(c).map_err(offline_adapter_error)?;
    r.actions.push(json!({"action":"auto_exit_requested","decision":format!("{:?}",review.decision),"modeled_net_after_reserve":net}));
   }
  }Ok(true)
 }
}
#[cfg(test)] mod auto_tests {
 use super::*;
 #[test] fn r1325_auto_exact_joint_steps(){assert_eq!(auto_lcm(20,30).unwrap(),60);assert!(auto_lcm(0,1).is_err());assert!(auto_lcm(u128::MAX,2).is_err());}
 #[test] fn r1325_auto_canonical_policy_preserved(){let p=AutoPolicy::load().unwrap();let d=p.decision().unwrap();assert_eq!(d.max_age_ms,800);assert_eq!(d.base_size,10.0);assert_eq!(d.required_net,42.0);assert!((d.cost-24.03).abs()<1e-10);assert_eq!(p.number("filters","min_24h_volume_usdt").unwrap(),150000.0);}
 #[test] fn r1325_auto_no_manifest_required(){let p=parse_runtime_plan(&["--paper-runtime=integrated-paper".into(),"--journal-root=/tmp/example".into()],&[]).unwrap();assert!(matches!(p.mode,RuntimeMode::IntegratedPaper{manifest:None,..}));}
}
#[cfg(test)] mod auto_metadata_tests {
 use super::*;
 #[test] fn r1325_auto_full_metadata_universe_no_fixed_cap(){
  let p=AutoPolicy::load().unwrap();
  let mut meta=core::market_metadata::NativeMetadataSnapshot{generation:7,loaded_ns:1,loaded_wall_ms:1,binance_active:256,binance_tradifi_excluded:0,okx_active:256,metadata_ready:256,static_denied:0,markets:BTreeMap::new(),routes:HashMap::new()};
  for n in 0..256{let b=format!("T{n}");let m=core::market_metadata::NativeMarketMetadata{base:b.clone(),spot_symbol:format!("{b}-USDT"),future_symbol:format!("{b}USDT"),spot_min_amount:Some("0.02".into()),future_min_base:Some("0.03".into()),spot_min_cost:"1".into(),future_min_cost:"5".into(),spot_step:Some("0.02".into()),future_step_base:Some("0.03".into()),contract_size:"1".into(),buy_exchange:"okx".into(),sell_exchange:"binance".into(),route_label:"test".into(),static_reason:None,loaded_ns:1,loaded_wall_ms:1,spot_tick:Some("0.01".into()),future_tick:Some("0.01".into()),spot_status:"live".into(),future_status:"TRADING".into(),spot_max_amount:Some("100".into()),future_max_base:Some("90".into())};meta.markets.insert(b,m);}
  let m=p.manifest(&meta,std::path::Path::new("/tmp/r1325-auto-test")).unwrap();validate_manifest(&m).unwrap();assert_eq!(m.instruments.len()+1,256);
  for id in std::iter::once(&m.identity).chain(&m.instruments){assert_eq!(id.profile.step,"0.06");assert_eq!(id.profile.minimum_quantity,"0.06");metadata_binding(&meta.markets[&id.profile.base],&id.profile).unwrap();}
  meta.markets.get_mut("T0").unwrap().static_reason=Some("MARKET_INACTIVE".into());
  let m=p.manifest(&meta,std::path::Path::new("/tmp/r1325-auto-test")).unwrap();assert_eq!(m.instruments.len()+1,255);
 }
}
#[cfg(test)] impl AutoPolicy {pub(super) fn seed_turnover(&self,base:&str,n:u64,w:i64){self.turnover.seed(base,n,w);}}
