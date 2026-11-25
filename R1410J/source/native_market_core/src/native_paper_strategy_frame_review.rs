//! Astra P1-06: deliberately supervised PUBLIC-PAPER config consumer.
//! Strictly in-memory modeled replay, not deployed; never sends private or
//! public orders and does not grant authority. Does NOT override R95 cutover.
//! Strategy decisions are checked before PaperPortfolio's legacy close path.
use crate::{Book,Venue};
use crate::native_trading_config::NativeTradingConfig;
use crate::native_legacy_strategy_policy_review::{
 LegacyCloseObservation,LegacyClosePreview,inspect_telemetry_velocity_readonly,
};
use crate::native_paper_market_execution_review::{PaperPhase,PaperMachine,PaperPosition};
use crate::native_paper_portfolio_review::PaperPortfolio;
use crate::decision::{Decision,RouteSpec,DecisionConfig};
use crate::position_lifecycle::CloseDecision;
use crate::native_prepared_exit_policy_review::PreparedExitPolicyReview;
use std::collections::{BTreeMap,BTreeSet};
use serde::{Serialize,Deserialize};
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum StrategyFrameError{Config,InvalidFrame,ManualEmergency,ManualUnknown,Portfolio}
#[derive(Debug,Clone,PartialEq)]
pub struct StrategyFrameEvidence{
 pub decision:Option<CloseDecision>,
 pub permitted_paper_legacy_model_update:bool,
 pub config_source_sha256:String,
 pub proof_of_real_execution:bool,
}
/// R110 Public market input for review only.
#[derive(Clone,Debug,PartialEq)]
pub struct PublicEntryMarketSnapshot{
 pub base:String,pub spot_volume_24h_quote:f64,
 pub futures_volume_24h_quote:f64,pub observed_wall_ms:i64,
}
#[derive(Clone,Debug)]
struct PublicMarketEntryBounds{min_turnover_quote:f64,max_abs_spread_bps:f64}
impl PublicMarketEntryBounds{
 fn compile(c:&NativeTradingConfig)->Result<Self,StrategyFrameError>{
  const ONE:f64=1_000_000_000_000_000_000.0;
  let min_turnover_quote=c.policy.min_market_volume_24h_quote_e18 as f64/ONE;
  let max_abs_spread_bps=c.policy.max_market_abs_spread_milli_bps as f64/1000.0;
  if !min_turnover_quote.is_finite()||min_turnover_quote<=0.0
    ||!max_abs_spread_bps.is_finite()||max_abs_spread_bps<=0.0{
    return Err(StrategyFrameError::Config)
  }
  Ok(Self{min_turnover_quote,max_abs_spread_bps})
 }
 fn admits(&self,base:&str,ask:f64,bid:f64,now_wall_ms:i64,
   market:Option<&PublicEntryMarketSnapshot>)->bool{
  let Some(m)=market else{return false};
  if m.base!=base||now_wall_ms<m.observed_wall_ms
    ||now_wall_ms-m.observed_wall_ms>60_000
    ||!m.spot_volume_24h_quote.is_finite()
    ||!m.futures_volume_24h_quote.is_finite()
    ||m.spot_volume_24h_quote<self.min_turnover_quote
    ||m.futures_volume_24h_quote<self.min_turnover_quote
    ||!ask.is_finite()||ask<=0.0||!bid.is_finite()||bid<=0.0{
    return false
  }
  let spread=(bid/ask-1.0)*10_000.0;
  spread.is_finite()&&spread.abs()<=self.max_abs_spread_bps
 }
}
fn same_decision_profile(a:&DecisionConfig,b:&DecisionConfig)->bool{
 a.max_age_ms==b.max_age_ms
  &&a.metadata_ttl_ms==b.metadata_ttl_ms
  &&a.base_size.to_bits()==b.base_size.to_bits()
  &&a.risk_cap.to_bits()==b.risk_cap.to_bits()
  &&a.cost.to_bits()==b.cost.to_bits()
  &&a.required_net.to_bits()==b.required_net.to_bits()
  &&a.min_price.to_bits()==b.min_price.to_bits()
  &&a.max_price.to_bits()==b.max_price.to_bits()
  &&a.blacklist==b.blacklist
}
/// R102 in-memory replay transfer image only. NOT an attested durable
/// checkpoint, broker position recovery, restart authorization, or signature.
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyReviewImage{
 pub schema:u32,
 pub config_sha256:String,
 pub portfolio:PaperPortfolio,
 pub previous_public_spreads:BTreeMap<String,(f64,f64)>,
 pub quarantined_bases:BTreeSet<String>,
 pub real_trading_authorized:bool,
}
/// R103: YAML SOURCE and R93 source digest are verified once at admission.
/// Hot frames use private immutable typed settings, not repeated YAML parsing.
/// This model remains unauthenticated review only, NOT live trading permission.
pub struct SupervisedStrategyPaper{
 pub portfolio:PaperPortfolio,
 prepared:PreparedExitPolicyReview,
 entry_policy:DecisionConfig,
 source_entry_policy:DecisionConfig,
 market_bounds:PublicMarketEntryBounds,
 source_market_bounds:PublicMarketEntryBounds,
 source_quotas:crate::native_paper_config_bridge::SourcePaperQuotasReview,
 source_temporal:crate::native_paper_config_bridge::PaperTemporalPolicyReview,
 source_scan_cooldown_ns:u64,
 source_model:crate::native_paper_market_execution_review::PaperAssumptions,
 config_sha256:String,
 previous_public_spreads:BTreeMap<String,(f64,f64)>,
 // Fail-closed after a public emergency: do not resume virtual actions by
 // accident on the next frame or silently convert it to a profitable fill.
 quarantined_bases:BTreeSet<String>,
}
/// R104 static consistency check on an untrusted *research-only* replay image.
/// Replaying syntactically valid JSON must not manufacture modeled hedges.
fn paper_leg_valid(p:&PaperPosition,base:&str)->bool{
 p.base==base && !base.is_empty()
  &&p.signal_ns>0&&p.spot_buy_ns>=p.signal_ns
  &&p.spot_paid_quote.is_finite()&&p.spot_paid_quote>0.0
  &&p.spot_gross_base.is_finite()&&p.spot_gross_base>0.0
  &&p.spot_delivered_base.is_finite()&&p.spot_delivered_base>0.0
  &&p.spot_delivered_base<=p.spot_gross_base*(1.0+1e-10)
  &&p.signal_estimated_net_bps.is_finite()
}
fn modeled_hedge_valid(p:&PaperPosition)->bool{
 p.futures_short_ns.is_some_and(|t|t>p.spot_buy_ns)
 &&p.futures_short_base.is_some_and(|n|
   n.is_finite()&&n>0.0&&(n-p.spot_delivered_base).abs()<=p.spot_delivered_base*1e-8)
 &&p.futures_short_proceeds_quote.is_some_and(|n|n.is_finite()&&n>0.0)
 &&p.hedge_entry_estimated_net_bps.is_some_and(|n|n.is_finite())
}
fn active_model_valid(base:&str,m:&PaperMachine,a:&crate::native_paper_market_execution_review::PaperAssumptions)->bool{
 if m.config.quote_budget!=a.quote_budget||
  m.config.min_entry_net_bps!=a.min_entry_net_bps||
  m.config.other_cost_bps!=a.other_cost_bps||
  m.config.spot_buy_fee_base_bps!=a.spot_buy_fee_base_bps||
  m.config.max_book_age_ns!=a.max_book_age_ns||
  m.config.take_profit_quote.is_some_and(|n|!n.is_finite()||n<0.0){
  return false
 }
 match &m.phase{
  PaperPhase::AwaitingHedge(p)=>{
   paper_leg_valid(p,base)&&p.futures_short_ns.is_none()
    &&p.futures_short_base.is_none()
    &&p.futures_short_proceeds_quote.is_none()
    &&p.hedge_entry_estimated_net_bps.is_none()
  },
  PaperPhase::Hedged(p)=>paper_leg_valid(p,base)&&modeled_hedge_valid(p),
  PaperPhase::AwaitingSpotExit{pos,cover_quote,cover_ns,close_estimate_quote}=>{
   paper_leg_valid(pos,base)&&modeled_hedge_valid(pos)
    &&pos.futures_short_ns.is_some_and(|t|*cover_ns>t)
    &&cover_quote.is_finite()&&*cover_quote>0.0
    &&close_estimate_quote.is_finite()
  },
  // Flat and Closed must not be present in the active portfolio map.
  PaperPhase::Flat|PaperPhase::Closed(_)=>false,
 }
}
/// R108: reuse the R104 PAPER position-shape rules at BOTH strategy
/// restore and durable modeled-journal admission. These checks validate
/// internal consistency only, never independently attested exchange fills.
pub(crate) fn strategy_review_image_shape_valid(image:&StrategyReviewImage)->bool{
 let p=&image.portfolio;
 if image.schema!=1||image.real_trading_authorized
    ||!(1..=10).contains(&p.max_concurrent_positions)
    ||!(1..=10_000).contains(&p.max_daily_entries)
    ||p.positions.len()>p.max_concurrent_positions
    ||p.stats.entries>p.max_daily_entries
    ||!p.stats.modeled_realized_usdt.is_finite()
    ||p.stats.closed.checked_add(p.positions.len() as u64)
      .map_or(true,|total|total>p.stats.entries)
    ||p.stats.closed_wins.checked_add(p.stats.closed_losses)!=Some(p.stats.closed)
    ||!(1_000_000_000..=86_400_000_000_000).contains(&p.reentry_cooldown_ns)
    ||!(1_000_000_000..=86_400_000_000_000).contains(&p.scan_cooldown_ns)
    ||p.scan_last_mark_ns.iter().any(|(base,ns)|base.is_empty()||*ns==0)
    ||p.stats.futures_hedges>p.stats.entries
    ||p.stats.futures_covers>p.stats.futures_hedges
    ||p.stats.closed>p.stats.futures_covers
    ||p.closed_bases.iter().any(|base|p.positions.contains_key(base)
      ||!p.closed_at_monotonic_ns.contains_key(base))
    ||p.closed_at_monotonic_ns.iter().any(|(base,at)|
       !p.closed_bases.contains(base)||*at==0)
    ||crate::native_paper_market_execution_review::PaperMachine::new(
       p.assumptions.clone()).is_err(){
    return false
 }
 p.positions.iter().all(|(base,m)|active_model_valid(base,m,&p.assumptions))
}
impl SupervisedStrategyPaper{
 pub fn new(cfg:NativeTradingConfig,at_wall_ms:i64)->Result<Self,StrategyFrameError>{
  if !cfg.source_projection_unchanged()||cfg.policy.real_execution_authorized
    ||cfg.policy.real_daily_trades!=0
    // R174: unsupported legacy YAML controls cannot silently drift in the
    // source-owned research PAPER engine. This does not implement them.
    ||crate::native_r174_unwired_legacy_guard::verify_unwired_source_r174(&cfg).is_err()
    ||cfg.raw.get("general").and_then(|g|g.get("trade_enabled"))
      .and_then(|v|v.as_bool())!=Some(false)
    // These optional entry strategies are still READ-ONLY previews and
    // cannot silently size or target actual modeled PAPER entries.
    ||["adaptive_entry","dynamic_entry"].iter().any(|section|
      cfg.raw.get(*section).and_then(|s|s.get("enabled"))
        .and_then(|v|v.as_bool())!=Some(false))
    // This review engine supports source quotas, unlike the installed 500/day
    // supervisor. Reject unrelated enabled features it does not execute.
    ||["scale_in_hints","analytics"].iter().any(|section|
      cfg.raw.get(*section).and_then(|s|s.get("enabled"))
       .and_then(|v|v.as_bool())!=Some(false))
    // PreparedClose currently uses velocity thresholds unconditionally.
    // Until OFF semantics are implemented, do not misrepresent OFF as OFF.
    ||cfg.raw.get("velocity").and_then(|s|s.get("enabled"))
       .and_then(|v|v.as_bool())!=Some(true){
   return Err(StrategyFrameError::Config)
  }
  let model=crate::native_paper_model_config_review::PaperModelConfigReview::from_config(&cfg)
   .map_err(|_|StrategyFrameError::Config)?;
  let mut portfolio=PaperPortfolio::new(at_wall_ms)
   .map_err(|_|StrategyFrameError::Portfolio)?;
  // Unlike the old compiled fallback, this IN-MEMORY portfolio reads actual
  // source-bound modeled budget/cost/age. No live service is reconfigured.
  let source_model=model.model.clone();
  portfolio.assumptions=model.model;
  let quotas=crate::native_paper_config_bridge::bind_source_paper_quotas_review(&cfg)
   .map_err(|_|StrategyFrameError::Config)?;
  portfolio.apply_source_paper_quotas_review(quotas.daily,quotas.concurrent)
   .map_err(|_|StrategyFrameError::Config)?;
  let temporal=crate::native_paper_config_bridge::bind_public_paper_temporal_policy_review(&cfg)
   .map_err(|_|StrategyFrameError::Config)?;
  portfolio.apply_public_paper_temporal_policy_review(
    temporal.min_lifetime_ns,temporal.exit_spread_bps,
    temporal.reentry_cooldown_ns,temporal.net_target_bps)
   .map_err(|_|StrategyFrameError::Config)?;
  let read_cooldown=|section:&str,key:&str|cfg.raw.get(section)
   .and_then(|s|s.get(key)).and_then(|x|x.as_u64())
   .filter(|n|(1..=86_400).contains(n))
   .and_then(|n|n.checked_mul(1_000_000_000));
  let scan=read_cooldown("limits","symbol_cooldown_sec")
   .ok_or(StrategyFrameError::Config)?;
  portfolio.scan_cooldown_ns=scan;
  // Compile dynamic/velocity thresholds ONCE; discard the mutable YAML tree.
  // Frame processing reads typed, privately held fields only.
  let prepared=PreparedExitPolicyReview::compile(&cfg)
   .map_err(|_|StrategyFrameError::Config)?;
  let entry_policy=crate::native_paper_config_bridge::
   bind_public_paper_decision_config(&cfg)
    .map_err(|_|StrategyFrameError::Config)?;
  let market_bounds=PublicMarketEntryBounds::compile(&cfg)?;
  let config_sha256=cfg.sha256;
  Ok(Self{portfolio,prepared,source_entry_policy:entry_policy.clone(),entry_policy,
    source_market_bounds:market_bounds.clone(),market_bounds,source_quotas:quotas,
    source_temporal:temporal,source_scan_cooldown_ns:scan,source_model,
    config_sha256,previous_public_spreads:BTreeMap::new(),
    quarantined_bases:BTreeSet::new()})
 }
 fn source_profile_intact(&self)->bool{
  same_decision_profile(&self.entry_policy,&self.source_entry_policy)
   &&self.market_bounds.min_turnover_quote.to_bits()==self.source_market_bounds.min_turnover_quote.to_bits()
   &&self.market_bounds.max_abs_spread_bps.to_bits()==self.source_market_bounds.max_abs_spread_bps.to_bits()
   &&self.portfolio.max_daily_entries==self.source_quotas.daily
   &&self.portfolio.max_concurrent_positions==self.source_quotas.concurrent
   &&self.portfolio.scan_cooldown_ns==self.source_scan_cooldown_ns
   &&self.portfolio.assumptions.quote_budget.to_bits()==self.source_model.quote_budget.to_bits()
   &&self.portfolio.assumptions.min_entry_net_bps.to_bits()==self.source_model.min_entry_net_bps.to_bits()
   &&self.portfolio.assumptions.other_cost_bps.to_bits()==self.source_model.other_cost_bps.to_bits()
   &&self.portfolio.assumptions.spot_buy_fee_base_bps.to_bits()==self.source_model.spot_buy_fee_base_bps.to_bits()
   &&self.portfolio.assumptions.max_book_age_ns==self.source_model.max_book_age_ns
   &&self.portfolio.assumptions.take_profit_quote.map(f64::to_bits)==self.source_model.take_profit_quote.map(f64::to_bits)
   &&self.portfolio.verify_public_paper_temporal_policy_review(
     self.source_temporal.min_lifetime_ns,self.source_temporal.exit_spread_bps,
     self.source_temporal.reentry_cooldown_ns,self.source_temporal.net_target_bps).is_ok()
 }
 /// A6 review-only effective source report; installed binary identity is UNKNOWN.
 pub fn effective_research_config_report(&self)->Result<serde_json::Value,StrategyFrameError>{
  if !self.source_profile_intact()
     ||self.prepared.source_sha256()!=self.config_sha256{
   return Err(StrategyFrameError::Config)
  }
  Ok(serde_json::json!({
    "profile":"source-paper-review-v1",
    "source_sha256":self.config_sha256,
    "paper_max_daily_entries":self.source_quotas.daily,
    "paper_max_concurrent_positions":self.source_quotas.concurrent,
    "decision_budget_quote":self.source_entry_policy.base_size,
    "decision_risk_cap_quote":self.source_entry_policy.risk_cap,
    "decision_entry_required_net_bps":self.source_entry_policy.required_net,
    "decision_round_trip_cost_bps":self.source_entry_policy.cost,
    "decision_min_price_quote":self.source_entry_policy.min_price,
    "decision_max_price_quote":self.source_entry_policy.max_price,
    "decision_blacklist_count":self.source_entry_policy.blacklist.len(),
    "decision_max_book_age_ms":self.source_entry_policy.max_age_ms,
    "decision_metadata_ttl_ms":self.source_entry_policy.metadata_ttl_ms,
    "market_min_volume_quote_24h":self.source_market_bounds.min_turnover_quote,
    "market_max_abs_spread_bps":self.source_market_bounds.max_abs_spread_bps,
    "min_lifetime_ns":self.source_temporal.min_lifetime_ns,
    "exit_spread_bps":self.source_temporal.exit_spread_bps,
    "reentry_cooldown_ns":self.source_temporal.reentry_cooldown_ns,
    "scan_cooldown_ns":self.source_scan_cooldown_ns,
    "net_target_bps":self.source_temporal.net_target_bps,
    "model_quote_budget":self.source_model.quote_budget,
    "model_min_entry_net_bps":self.source_model.min_entry_net_bps,
    "model_other_cost_bps":self.source_model.other_cost_bps,
    "model_spot_buy_fee_base_bps":self.source_model.spot_buy_fee_base_bps,
    "model_max_book_age_ns":self.source_model.max_book_age_ns,
    "model_take_profit_quote":self.source_model.take_profit_quote,
    "review_source_quota_matches_compiled_legacy_default":
      self.source_quotas.daily==crate::native_paper_portfolio_review::MAX_DAILY_PAPER as u64
      &&self.source_quotas.concurrent==
        crate::native_paper_portfolio_review::MAX_CONCURRENT_PAPER,
    "compiled_legacy_default_daily_cap":
      crate::native_paper_portfolio_review::MAX_DAILY_PAPER,
    "public_ws_transport_attested":false,
    "exchange_fills_attested":false,
    "replay_image_is_durable":false,
    "deployed_binary_sha256":null,
    "installed_runtime_source_bound":false,
    "real_trading_authorized":false
  }))
 }
 /// A transport-neutral REVIEW snapshot for deterministic tests only.
 /// Saving it to disk alone is NOT crash-safe or proof of exchange truth.
 pub fn replay_image(&self)->Result<StrategyReviewImage,StrategyFrameError>{
  if !self.source_profile_intact()
     ||self.prepared.source_sha256()!=self.config_sha256{
   return Err(StrategyFrameError::Config)
  }
  Ok(StrategyReviewImage{
   schema:1,config_sha256:self.config_sha256.clone(),
   portfolio:self.portfolio.clone(),
   previous_public_spreads:self.previous_public_spreads.clone(),
   quarantined_bases:self.quarantined_bases.clone(),
   real_trading_authorized:false,
  })
 }
 /// Restore the exact in-memory decision history in a *controlled fixture*.
 /// This is NOT the disk-fsynced journal / owner recovery P0 requires.
 pub fn from_replay_image(
  cfg:NativeTradingConfig,at_wall_ms:i64,image:StrategyReviewImage
 )->Result<Self,StrategyFrameError>{
  let mut fresh=Self::new(cfg,at_wall_ms)?;
  if image.schema!=1||image.real_trading_authorized
    ||!strategy_review_image_shape_valid(&image)
    ||image.config_sha256!=fresh.config_sha256
    ||image.portfolio.day_yek!=fresh.portfolio.day_yek
    ||image.portfolio.max_daily_entries!=fresh.portfolio.max_daily_entries
    ||image.portfolio.max_concurrent_positions!=fresh.portfolio.max_concurrent_positions
    ||image.portfolio.positions.len()>fresh.portfolio.max_concurrent_positions
    ||image.portfolio.stats.entries>fresh.portfolio.max_daily_entries
    ||!image.portfolio.stats.modeled_realized_usdt.is_finite()
    // R104: reject replayed models whose lifecycle/counters cannot exist.
    ||image.portfolio.stats.closed.checked_add(
        image.portfolio.positions.len() as u64)
       .map_or(true,|count|count>image.portfolio.stats.entries)
    ||image.portfolio.stats.closed_wins.checked_add(
        image.portfolio.stats.closed_losses)
       !=Some(image.portfolio.stats.closed)
    ||image.portfolio.stats.futures_hedges>image.portfolio.stats.entries
    ||image.portfolio.stats.futures_covers>image.portfolio.stats.futures_hedges
    ||image.portfolio.stats.closed>image.portfolio.stats.futures_covers
    ||image.portfolio.positions.iter().any(|(base,m)|
       !active_model_valid(base,m,&fresh.portfolio.assumptions))
    ||image.quarantined_bases.iter().any(|k|
      !image.portfolio.positions.contains_key(k))
    ||image.previous_public_spreads.iter().any(|(k,(spread,at))|
      !image.portfolio.positions.contains_key(k)
       ||!spread.is_finite()||spread.abs()>1_000_000.0
       ||!at.is_finite()||*at<0.0)
    ||image.portfolio.reentry_cooldown_ns!=fresh.portfolio.reentry_cooldown_ns
    ||image.portfolio.scan_cooldown_ns!=fresh.portfolio.scan_cooldown_ns
    ||image.portfolio.min_lifetime_ns!=fresh.portfolio.min_lifetime_ns
    ||image.portfolio.exit_spread_bps.to_bits()!=fresh.portfolio.exit_spread_bps.to_bits()
    ||image.portfolio.net_target_bps.to_bits()!=fresh.portfolio.net_target_bps.to_bits()
    ||serde_json::to_value(&image.portfolio.assumptions).ok()!=
      serde_json::to_value(&fresh.portfolio.assumptions).ok(){
   return Err(StrategyFrameError::Config)
  }
  fresh.portfolio=image.portfolio;
  fresh.previous_public_spreads=image.previous_public_spreads;
  fresh.quarantined_bases=image.quarantined_bases;
  Ok(fresh)
 }
 /// Price books and timestamp must come from SAME validated public frame.
 /// No future frames, signed exchange truth or private balances involved.
 pub fn on_frame_review(
  &mut self,base:&str,venue:Venue,now_ns:u64,now_wall_ms:i64,
  spot:Option<&Book>,fut:Option<&Book>,
  decision:Option<&Decision>,route:Option<&RouteSpec>
 )->Result<StrategyFrameEvidence,StrategyFrameError>{
  self.on_frame_review_with_market(base,venue,now_ns,now_wall_ms,spot,fut,decision,route,None)
 }
 /// R117 source-shaped public ticker preview. No original transport attestation.
 /// Existing owned virtual positions keep progressing without new entry data.
 pub fn on_frame_review_with_source_tickers(
  &mut self,base:&str,venue:Venue,now_ns:u64,now_wall_ms:i64,
  spot:Option<&Book>,fut:Option<&Book>,
  decision:Option<&Decision>,route:Option<&RouteSpec>,
  okx:crate::native_public_turnover_source_review::PublicTickerSource<'_>,
  binance:crate::native_public_turnover_source_review::PublicTickerSource<'_>,
 )->Result<StrategyFrameEvidence,StrategyFrameError>{
  if self.portfolio.positions.contains_key(base){
   return self.on_frame_review(base,venue,now_ns,now_wall_ms,spot,fut,decision,route)
  }
  let source=crate::native_public_turnover_source_review::review_public_ticker_pair(
    base,now_wall_ms,okx,binance).map_err(|_|StrategyFrameError::InvalidFrame)?;
  self.on_frame_review_with_market(base,venue,now_ns,now_wall_ms,
    spot,fut,decision,route,Some(&source.market))
 }
 /// R122: additional same-boot source continuity; still untrusted
 /// without wiring an actual TLS-verified public transport owner.
 pub fn on_frame_review_with_continuity_tickers(
  &mut self,base:&str,venue:Venue,now_ns:u64,now_wall_ms:i64,
  spot:Option<&Book>,fut:Option<&Book>,
  decision:Option<&Decision>,route:Option<&RouteSpec>,
  continuity:&mut crate::native_public_turnover_continuity_r122_review::PublicTurnoverContinuityR122,
  okx:crate::native_public_turnover_source_review::PublicTickerSource<'_>,
  binance:crate::native_public_turnover_source_review::PublicTickerSource<'_>,
 )->Result<StrategyFrameEvidence,StrategyFrameError>{
  if self.portfolio.positions.contains_key(base){
   return self.on_frame_review(base,venue,now_ns,now_wall_ms,spot,fut,decision,route)
  }
  let source=continuity.accept_shaped_pair(base,now_wall_ms,okx,binance)
    .map_err(|_|StrategyFrameError::InvalidFrame)?;
  self.on_frame_review_with_market(base,venue,now_ns,now_wall_ms,
    spot,fut,decision,route,Some(&source.market))
 }
 /// R130: require a second distinct timestamp from BOTH source-shaped
 /// tickers after cold boot/reconnect, plus R126 owner topology. Research only.
 pub fn on_frame_review_with_owner_cold_start(
  &mut self,base:&str,venue:Venue,now_ns:u64,now_wall_ms:i64,
  spot:Option<&Book>,fut:Option<&Book>,
  decision:Option<&Decision>,route:Option<&RouteSpec>,
  owner:&crate::native_public_ws_owner_topology_r126_review::PublicTickerTopologyR126,
  spot_connection:&str,fut_connection:&str,
  boot:&mut crate::native_public_cold_boot_r130_review::PublicColdBootR130,
  okx:crate::native_public_turnover_ws_r123_review::PublicWsEnvelope<'_>,
  binance:crate::native_public_turnover_ws_r123_review::PublicWsEnvelope<'_>,
 )->Result<StrategyFrameEvidence,StrategyFrameError>{
  if self.portfolio.positions.contains_key(base){
   return self.on_frame_review(base,venue,now_ns,now_wall_ms,spot,fut,decision,route)
  }
  owner.check_pair(spot_connection,fut_connection,okx,binance)
   .map_err(|_|StrategyFrameError::InvalidFrame)?;
  let snapshot=boot.observe(now_wall_ms,okx,binance)
   .map_err(|_|StrategyFrameError::InvalidFrame)?;
  self.on_frame_review_with_market(base,venue,now_ns,now_wall_ms,
   spot,fut,decision,route,snapshot.as_ref().map(|x|&x.snapshot))
 }
 /// R151 B1/B2: compose the existing R126 owner and R130 cold boot
 /// with R150's source-owned entry decision. No caller-supplied Decision can
 /// bypass the YAML filters, missing two-venue turnover, or warm-up rules.
 /// All envelopes remain caller-shaped; this is NOT authenticated WS I/O.
 pub fn on_source_bound_frame_with_owner_cold_start(
  &mut self,base:&str,venue:Venue,now_ns:u64,now_wall_ms:i64,
  spot:Option<&Book>,fut:Option<&Book>,route:Option<&RouteSpec>,
  owner:&crate::native_public_ws_owner_topology_r126_review::PublicTickerTopologyR126,
  spot_connection:&str,fut_connection:&str,
  boot:&mut crate::native_public_cold_boot_r130_review::PublicColdBootR130,
  okx:crate::native_public_turnover_ws_r123_review::PublicWsEnvelope<'_>,
  binance:crate::native_public_turnover_ws_r123_review::PublicWsEnvelope<'_>,
 )->Result<StrategyFrameEvidence,StrategyFrameError>{
  if self.portfolio.positions.contains_key(base){
   return self.on_source_bound_frame_review_with_market(
    base,venue,now_ns,now_wall_ms,spot,fut,route,None)
  }
  owner.check_pair(spot_connection,fut_connection,okx,binance)
   .map_err(|_|StrategyFrameError::InvalidFrame)?;
  let snapshot=boot.observe(now_wall_ms,okx,binance)
   .map_err(|_|StrategyFrameError::InvalidFrame)?;
  self.on_source_bound_frame_review_with_market(
    base,venue,now_ns,now_wall_ms,spot,fut,route,
    snapshot.as_ref().map(|p|&p.snapshot))
 }
 /// R129: research-only owner/topology continuity chain, NOT a real network permit.
 pub fn on_frame_review_with_owned_ws_tickers(
  &mut self,base:&str,venue:Venue,now_ns:u64,now_wall_ms:i64,
  spot:Option<&Book>,fut:Option<&Book>,
  decision:Option<&Decision>,route:Option<&RouteSpec>,
  owner:&crate::native_public_ws_owner_topology_r126_review::PublicTickerTopologyR126,
  spot_connection:&str,fut_connection:&str,
  continuity:&mut crate::native_public_turnover_ws_continuity_r124_review::WsTurnoverContinuityR124,
  okx:crate::native_public_turnover_ws_r123_review::PublicWsEnvelope<'_>,
  binance:crate::native_public_turnover_ws_r123_review::PublicWsEnvelope<'_>,
 )->Result<StrategyFrameEvidence,StrategyFrameError>{
  if !self.portfolio.positions.contains_key(base){
   owner.check_pair(spot_connection,fut_connection,okx,binance)
    .map_err(|_|StrategyFrameError::InvalidFrame)?;
  }
  self.on_frame_review_with_ws_tickers(base,venue,now_ns,now_wall_ms,
   spot,fut,decision,route,continuity,okx,binance)
 }
 /// R124 offline WS quote-volume parity, never a live admission permit.
 pub fn on_frame_review_with_ws_tickers(
  &mut self,base:&str,venue:Venue,now_ns:u64,now_wall_ms:i64,
  spot:Option<&Book>,fut:Option<&Book>,
  decision:Option<&Decision>,route:Option<&RouteSpec>,
  continuity:&mut crate::native_public_turnover_ws_continuity_r124_review::WsTurnoverContinuityR124,
  okx:crate::native_public_turnover_ws_r123_review::PublicWsEnvelope<'_>,
  binance:crate::native_public_turnover_ws_r123_review::PublicWsEnvelope<'_>,
 )->Result<StrategyFrameEvidence,StrategyFrameError>{
  if self.portfolio.positions.contains_key(base){
   return self.on_frame_review(base,venue,now_ns,now_wall_ms,spot,fut,decision,route)
  }
  let m=continuity.accept(now_wall_ms,okx,binance)
    .map_err(|_|StrategyFrameError::InvalidFrame)?;
  self.on_frame_review_with_market(base,venue,now_ns,now_wall_ms,
    spot,fut,decision,route,Some(&m.snapshot))
 }
 /// R150 A4/B1: one immutable source policy owns the decision and the
 /// virtual portfolio transition. The caller supplies only PUBLIC market
 /// facts and route metadata, never a pre-approved entry Decision.
 /// Until public turnover is present, a new entry remains fail-closed.
 /// This is an in-memory research path, not the installed PAPER binary.
 pub fn on_source_bound_frame_review_with_market(
  &mut self,base:&str,venue:Venue,now_ns:u64,now_wall_ms:i64,
  spot:Option<&Book>,fut:Option<&Book>,route:Option<&RouteSpec>,
  market:Option<&PublicEntryMarketSnapshot>
 )->Result<StrategyFrameEvidence,StrategyFrameError>{
  if !self.source_profile_intact()
    ||self.prepared.source_sha256()!=self.config_sha256{
   return Err(StrategyFrameError::Config)
  }
  // Never invent a new signal for an already owned virtual position:
  // its exit, hedge and quarantine continue on the existing state machine.
  let proposal=if self.portfolio.positions.contains_key(base){
   None
  }else{
   match (spot,fut,route){
    (Some(s),Some(f),Some(r))=>Some(crate::decision::evaluate(
      base,r,Some(s),Some(f),now_ns,now_wall_ms,&self.entry_policy)),
    _=>None,
   }
  };
  self.on_frame_review_with_market(base,venue,now_ns,now_wall_ms,
    spot,fut,proposal.as_ref(),route,market)
 }
 pub fn on_frame_review_with_market(
  &mut self,base:&str,venue:Venue,now_ns:u64,now_wall_ms:i64,
  spot:Option<&Book>,fut:Option<&Book>,
  decision:Option<&Decision>,route:Option<&RouteSpec>,
  market:Option<&PublicEntryMarketSnapshot>
 )->Result<StrategyFrameEvidence,StrategyFrameError>{
  if !self.source_profile_intact()
     ||self.prepared.source_sha256()!=self.config_sha256{
   return Err(StrategyFrameError::Config)
  }
  if self.quarantined_bases.contains(base){
   return Err(StrategyFrameError::ManualEmergency)
  }
  self.portfolio.day_check(now_wall_ms)
   .map_err(|_|StrategyFrameError::Portfolio)?;
  let mut result=StrategyFrameEvidence{
   decision:None,permitted_paper_legacy_model_update:false,
   config_source_sha256:self.config_sha256.clone(),
   proof_of_real_execution:false,
  };
  let is_hedged=self.portfolio.positions.get(base)
    .is_some_and(|m|matches!(m.phase,PaperPhase::Hedged(_)));
  if !is_hedged{
   if !self.portfolio.positions.contains_key(base){
    // R109: enforce CURRENT source YAML filters at the last modeled entry
    // boundary; do not trust a caller-supplied executable decision. These
    // checks NEVER impede completion of an already owned virtual position.
    let (Some(s),Some(f),Some(r),Some(proposed))=(spot,fut,route,decision)
       else{return Ok(result)};
    if proposed.reason!="EXECUTABLE_OPPORTUNITY"{return Ok(result)}
    if !self.market_bounds.admits(base,
      s.asks.first().map(|q|q.0).unwrap_or(f64::NAN),
      f.bids.first().map(|q|q.0).unwrap_or(f64::NAN),
      now_wall_ms,market){return Err(StrategyFrameError::InvalidFrame)}
    if s.bids.is_empty()||s.asks.is_empty()||
       f.bids.is_empty()||f.asks.is_empty()||
       [s,f].iter().any(|b|b.bids.iter().chain(&b.asks)
        .any(|(price,size)|!price.is_finite()||*price<=0.0
          ||!size.is_finite()||*size<=0.0))
       ||s.bids[0].0>s.asks[0].0||f.bids[0].0>f.asks[0].0
       ||now_ns<s.received_ns||now_ns<f.received_ns{
       return Err(StrategyFrameError::InvalidFrame)
    }
    let checked=crate::decision::evaluate(base,r,Some(s),Some(f),
       now_ns,now_wall_ms,&self.entry_policy);
    if checked.reason!="EXECUTABLE_OPPORTUNITY"
       ||checked.base!=proposed.base||checked.route!=proposed.route
       ||checked.buy_exchange!=proposed.buy_exchange
       ||checked.sell_exchange!=proposed.sell_exchange
       ||checked.quantity!=proposed.quantity
       ||checked.spot_vwap!=proposed.spot_vwap
       ||checked.futures_vwap!=proposed.futures_vwap
       ||checked.estimated_net_bps!=proposed.estimated_net_bps
       ||checked.spot_notional_usdt!=proposed.spot_notional_usdt
       ||checked.futures_notional_usdt!=proposed.futures_notional_usdt
       ||checked.metadata_state!=proposed.metadata_state
       ||checked.cost_bps!=proposed.cost_bps{
       return Err(StrategyFrameError::InvalidFrame)
    }
    self.portfolio.on_frame(base,venue,now_ns,now_wall_ms,
       Some(s),Some(f),Some(&checked),Some(r))
        .map_err(|_|StrategyFrameError::Portfolio)?;
   }else{
    self.portfolio.on_frame(base,venue,now_ns,now_wall_ms,
       spot,fut,decision,route).map_err(|_|StrategyFrameError::Portfolio)?;
   }
   result.permitted_paper_legacy_model_update=true;
   return Ok(result)
  }
  let spot=spot.ok_or(StrategyFrameError::InvalidFrame)?;
  let fut=fut.ok_or(StrategyFrameError::InvalidFrame)?;
  if now_ns<spot.received_ns||now_ns<fut.received_ns
   ||now_ns-spot.received_ns>self.portfolio.assumptions.max_book_age_ns
   ||now_ns-fut.received_ns>self.portfolio.assumptions.max_book_age_ns{
   return Err(StrategyFrameError::InvalidFrame)
  }
  let (spot_bid,spot_ask)=(
   spot.bids.first().map(|q|q.0).ok_or(StrategyFrameError::InvalidFrame)?,
   spot.asks.first().map(|q|q.0).ok_or(StrategyFrameError::InvalidFrame)?
  );
  let (fut_bid,fut_ask)=(
   fut.bids.first().map(|q|q.0).ok_or(StrategyFrameError::InvalidFrame)?,
   fut.asks.first().map(|q|q.0).ok_or(StrategyFrameError::InvalidFrame)?
  );
  if [spot_bid,spot_ask,fut_bid,fut_ask].iter()
    .any(|n|!n.is_finite()||*n<=0.0)
   ||spot_bid>spot_ask||fut_bid>fut_ask{
   return Err(StrategyFrameError::InvalidFrame)
  }
  let observed=(fut_bid/spot_ask-1.0)*10_000.0;
  if !observed.is_finite(){return Err(StrategyFrameError::InvalidFrame)}
  // Always account for only progressively newer monotonic observations.
  let time_s=now_ns as f64/1_000_000_000.0;
  let previous=self.previous_public_spreads.get(base).copied();
  if previous.is_some_and(|(_,last)|time_s<=last){
   return Err(StrategyFrameError::InvalidFrame)
  }
  let velocity=inspect_telemetry_velocity_readonly(observed,previous,time_s)
   .map_err(|_|StrategyFrameError::InvalidFrame)?;
  let machine=&self.portfolio.positions[base];
  let PaperPhase::Hedged(ref pos)=machine.phase else{
   return Err(StrategyFrameError::InvalidFrame)
  };
  if now_ns<pos.spot_buy_ns||pos.spot_delivered_base<=0.0
    ||!pos.spot_delivered_base.is_finite(){
   return Err(StrategyFrameError::ManualUnknown)
  }
  // Python _close_executable_spread_bps uses orderbook VWAP at the
  // entire close quantity, NOT best ask / best bid. Both depth sweeps must
  // succeed; unknown/partial hedge quantity is a manual state.
  let qty=pos.spot_delivered_base;
  if pos.futures_short_base.map_or(true,|q|
     !q.is_finite()||(q-qty).abs()>qty*1e-8)
     ||pos.futures_short_proceeds_quote.map_or(true,|q|!q.is_finite()||q<=0.0){
   return Err(StrategyFrameError::ManualUnknown)
  }
  let spot_sale=crate::native_paper_market_execution_review::sell_base(&spot.bids,qty)
   .map_err(|_|StrategyFrameError::InvalidFrame)?;
  let fut_cover=crate::native_paper_market_execution_review::buy_base(&fut.asks,qty)
   .map_err(|_|StrategyFrameError::InvalidFrame)?;
  let cover=(fut_cover/spot_sale-1.0)*10_000.0;
  if !cover.is_finite(){return Err(StrategyFrameError::InvalidFrame)}
  // Quantity here is only a checked, positive modeled base number. It is
  // not signed/exchange-attested exposure and can never authorize an order.
  let units=(qty*1_000_000_000_000.0).ceil();
  if !units.is_finite()||units<1.0||units>=u128::MAX as f64{
   return Err(StrategyFrameError::ManualUnknown)
  }
  let obs=LegacyCloseObservation{
   observed_spread_bps:observed,
   executable_close_spread_bps:Some(cover),
   velocity_bps_s:velocity,
   elapsed_seconds:(now_ns-pos.spot_buy_ns)/1_000_000_000,
   position_units:units as u128,
   force_close:false,futures_emergency_stop_hit:false,
  };
  let preview:LegacyClosePreview=self.prepared.inspect(obs)
   .map_err(|_|StrategyFrameError::Config)?;
  self.previous_public_spreads.insert(base.into(),(observed,time_s));
  result.decision=Some(preview.decision);
  match preview.decision {
   CloseDecision::CloseEmergency=>{
    self.quarantined_bases.insert(base.to_owned());
    return Err(StrategyFrameError::ManualEmergency)
   },
   CloseDecision::ManualRequired=>return Err(StrategyFrameError::ManualUnknown),
   CloseDecision::Wait|CloseDecision::Hold=>return Ok(result),
   CloseDecision::CloseNormal=>{}
  }
  // A normal LEGACY close signal alone is NOT enough to claim a profitable
  // virtual fill: retain existing two-observation modeled execution ordering.
  self.portfolio.on_frame_with_checked_exit_policy(base,venue,now_ns,now_wall_ms,
     Some(spot),Some(fut),decision,route,
     Some((preview.dynamic_min_lifetime_sec.checked_mul(1_000_000_000)
       .ok_or(StrategyFrameError::Config)?,preview.dynamic_exit_bps)))
      .map_err(|_|StrategyFrameError::Portfolio)?;
  result.permitted_paper_legacy_model_update=true;
  Ok(result)
 }
}

#[cfg(test)]
mod tests{
 use super::*;
 use crate::native_paper_market_execution_review::{PaperMachine,PaperPosition,PaperAssumptions};
 const WALL:i64=1_791_783_600_000;
 fn fixture_cfg()->NativeTradingConfig{
  // Synthetic quota matches compiled PAPER; leave operator YAML untouched.
  let src=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  let fixture=src.replacen("paper_max_trades_per_day: 3000",
                          "paper_max_trades_per_day: 500",1)
    .replacen("scale_in_hints:\n  enabled: true",
              "scale_in_hints:\n  enabled: false",1)
    .replacen("analytics:\n  enabled: true",
              "analytics:\n  enabled: false",1);
  assert_ne!(src,fixture);
  NativeTradingConfig::parse(&fixture).unwrap()
 }
 fn new_session()->SupervisedStrategyPaper{
  let mut session=SupervisedStrategyPaper::new(fixture_cfg(),WALL).unwrap();
  let mut machine=PaperMachine::new(PaperAssumptions::default()).unwrap();
  machine.phase=PaperPhase::Hedged(PaperPosition{
   base:"X".into(),signal_ns:1_000_000_000,
   spot_buy_ns:10_000_000_000,signal_estimated_net_bps:50.0,
   spot_gross_base:0.1,spot_delivered_base:0.1,spot_paid_quote:10.0,
   futures_short_ns:Some(10_000_000_001),
   futures_short_proceeds_quote:Some(10.1),
   futures_short_base:Some(0.1),hedge_entry_estimated_net_bps:Some(50.0)
  });
  session.portfolio.positions.insert("X".into(),machine);
  // Modeled fixture has already observed one entry and one hedge.
  session.portfolio.stats.entries=1;
  session.portfolio.stats.futures_hedges=1;
  session
 }
 fn book(ns:u64,bid:f64,ask:f64)->Book{
  Book{bids:vec![(bid,10.0)],asks:vec![(ask,10.0)],
   received_ns:ns,received_wall_ms:WALL,
   exchange_ts_ms:None,changed_ns:ns,epoch:1,sequence:Some(ns as i64)}
 }
 fn entry_route(ns:u64)->RouteSpec{
  let lim=crate::decision::Limits{
    min_amount:"0.001".into(),step:"0.001".into(),
    min_cost:"1".into(),max_amount:Some("1000".into())
  };
  RouteSpec{buy_exchange:"okx".into(),sell_exchange:"binance".into(),
   label:"okx-to-binance".into(),buy:lim.clone(),sell:lim,
   loaded_ns:ns,metadata_state:"SIZE_METADATA_READY".into()}
 }
 fn entry_market(base:&str)->PublicEntryMarketSnapshot{
  PublicEntryMarketSnapshot{base:base.into(),spot_volume_24h_quote:200_000.0,
   futures_volume_24h_quote:200_000.0,observed_wall_ms:WALL}
 }
 fn entry_books(ns:u64,price:f64)->(Book,Book){
  let mut spot=book(ns,price*0.99,price);
  spot.bids[0].1=1000.0;spot.asks[0].1=1000.0;
  let mut fut=book(ns,price*1.01,price*1.02);
  fut.bids[0].1=1000.0;fut.asks[0].1=1000.0;
  (spot,fut)
 }
 fn suggested(base:&str,route:&RouteSpec,s:&Book,f:&Book,ns:u64)
   ->Decision{
  let cfg=fixture_cfg();
  let policy=crate::native_paper_config_bridge::
    bind_public_paper_decision_config(&cfg).unwrap();
  crate::decision::evaluate(base,route,Some(s),Some(f),ns,WALL,&policy)
 }
 #[test]
 fn r151_source_owned_cold_ws_pair_opens_once_and_restores_only_same_yaml(){
  use crate::native_public_turnover_ws_r123_review::PublicWsEnvelope;
  use crate::native_public_cold_boot_r130_review::PublicColdBootR130;
  use crate::native_public_ws_owner_topology_r126_review::{
   TickerSubscription,PublicTickerTopologyR126,
  };
  let owner_id=crate::owner_index("BTC",12);
  let a_url="wss://ws.okx.com:8443/ws/v5/public";
  let b_url="wss://fstream.binance.com/market/ws/btcusdt@ticker";
  let owner=PublicTickerTopologyR126::new_with_owner_count(owner_id,12,&[
   TickerSubscription{connection_id:"okx".into(),owner:owner_id,
    venue:Venue::OkxSpot,base:"BTC".into(),url:a_url.into()},
   TickerSubscription{connection_id:"bn".into(),owner:owner_id,
    venue:Venue::BinanceFutures,base:"BTC".into(),url:b_url.into()},
  ]).unwrap();
  let cfg=fixture_cfg();
  let mut paper=SupervisedStrategyPaper::new(cfg.clone(),WALL).unwrap();
  let mut boot=PublicColdBootR130::default();
  let ns=40_000_000_000;
  let (spot,fut)=entry_books(ns,100.0);
  let route=entry_route(ns);
  let okx=|ts:i64|format!(
   r#"{{"arg":{{"channel":"tickers","instId":"BTC-USDT"}},"data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"250000","ts":"{ts}"}}]}}"#);
  let bin=|ts:i64|format!(
   r#"{{"e":"24hrTicker","E":{ts},"s":"BTCUSDT","q":"300000","O":{},"C":{ts},"st":1}}"#,
   ts-86_400_000);
  let a1=okx(WALL-1200);let b1=bin(WALL-1200);
  let a2=okx(WALL-800);let b2=bin(WALL-800);
  fn env<'a>(venue:Venue,url:&'a str,seq:u64,body:&'a str)->PublicWsEnvelope<'a>{
   PublicWsEnvelope{venue,url,base:"BTC",connection_epoch:3,sequence:seq,
    received_wall_ms:WALL,body}
  }
  let first=paper.on_source_bound_frame_with_owner_cold_start(
   "BTC",Venue::OkxSpot,ns,WALL,Some(&spot),Some(&fut),
   Some(&route),&owner,"okx","bn",&mut boot,
   env(Venue::OkxSpot,a_url,10,&a1),
   env(Venue::BinanceFutures,b_url,10,&b1));
  assert_eq!(first.err(),Some(StrategyFrameError::InvalidFrame));
  assert_eq!(paper.portfolio.stats.entries,0);
  assert!(!boot.ready("BTC"));
  let wrong=paper.on_source_bound_frame_with_owner_cold_start(
   "BTC",Venue::OkxSpot,ns,WALL,Some(&spot),Some(&fut),
   Some(&route),&owner,"forged","bn",&mut boot,
   env(Venue::OkxSpot,a_url,11,&a2),
   env(Venue::BinanceFutures,b_url,11,&b2));
  assert_eq!(wrong.err(),Some(StrategyFrameError::InvalidFrame));
  assert_eq!(paper.portfolio.stats.entries,0);
  assert!(!boot.ready("BTC"));
  let second=paper.on_source_bound_frame_with_owner_cold_start(
   "BTC",Venue::OkxSpot,ns,WALL,Some(&spot),Some(&fut),
   Some(&route),&owner,"okx","bn",&mut boot,
   env(Venue::OkxSpot,a_url,11,&a2),
   env(Venue::BinanceFutures,b_url,11,&b2)).unwrap();
  assert!(boot.ready("BTC"));
  assert!(second.permitted_paper_legacy_model_update);
  assert!(!second.proof_of_real_execution);
  assert_eq!(second.config_source_sha256,cfg.sha256);
  assert_eq!(paper.portfolio.stats.entries,1);
  let image=paper.replay_image().unwrap();
  let restored=SupervisedStrategyPaper::from_replay_image(
   cfg.clone(),WALL,image.clone()).unwrap();
  assert_eq!(restored.portfolio.stats.entries,1);
  let mut shifted=cfg.clone();
  shifted.raw["spread"]["entry_bps"]=serde_yaml::Value::from(90.0);
  assert!(SupervisedStrategyPaper::from_replay_image(
   shifted,WALL,image).is_err());
 }
 #[test]
 fn r130_first_ws_pair_cannot_admit_new_paper_entry(){
  use crate::native_public_turnover_ws_r123_review::PublicWsEnvelope;
  use crate::native_public_cold_boot_r130_review::PublicColdBootR130;
  use crate::native_public_ws_owner_topology_r126_review::{TickerSubscription,PublicTickerTopologyR126};
  let owner=crate::owner_index("BTC",3);
  let a_url="wss://ws.okx.com:8443/ws/v5/public";
  let b_url="wss://fstream.binance.com/market/ws/btcusdt@ticker";
  let owner=PublicTickerTopologyR126::new(owner,&[
   TickerSubscription{connection_id:"okx".into(),owner,venue:Venue::OkxSpot,
    base:"BTC".into(),url:a_url.into()},
   TickerSubscription{connection_id:"bn".into(),owner,venue:Venue::BinanceFutures,
    base:"BTC".into(),url:b_url.into()}
  ]).unwrap();
  let mut gate=PublicColdBootR130::default();
  let mut paper=SupervisedStrategyPaper::new(fixture_cfg(),WALL).unwrap();
  let t=40_000_000_000;
  let (s,f)=entry_books(t,100.0);
  let route=entry_route(t);
  let decision=suggested("BTC",&route,&s,&f,t);
  let a_raw=format!(r#"{{"arg":{{"channel":"tickers","instId":"BTC-USDT"}},"data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"250000","ts":"{}"}}]}}"#,WALL-1200);
  let b_raw=format!(r#"{{"e":"24hrTicker","E":{},"s":"BTCUSDT","q":"300000","O":{},"C":{},"st":1}}"#,WALL-1200,WALL-86_401_200,WALL-1200);
  let a=PublicWsEnvelope{venue:Venue::OkxSpot,url:a_url,base:"BTC",
   connection_epoch:1,sequence:1,received_wall_ms:WALL,body:&a_raw};
  let b=PublicWsEnvelope{venue:Venue::BinanceFutures,url:b_url,base:"BTC",
   connection_epoch:1,sequence:1,received_wall_ms:WALL,body:&b_raw};
  let first=paper.on_frame_review_with_owner_cold_start(
   "BTC",Venue::OkxSpot,t,WALL,Some(&s),Some(&f),
   Some(&decision),Some(&route),&owner,"okx","bn",&mut gate,a,b);
  assert!(matches!(first,Err(StrategyFrameError::InvalidFrame)));
  assert_eq!(paper.portfolio.stats.entries,0);
  assert!(!gate.ready("BTC"));
  let a_new=format!(r#"{{"arg":{{"channel":"tickers","instId":"BTC-USDT"}},"data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"250001","ts":"{}"}}]}}"#,WALL-800);
  let b_new=format!(r#"{{"e":"24hrTicker","E":{},"s":"BTCUSDT","q":"300001","O":{},"C":{},"st":1}}"#,WALL-800,WALL-86_400_800,WALL-800);
  let second=paper.on_frame_review_with_owner_cold_start(
   "BTC",Venue::OkxSpot,t,WALL,Some(&s),Some(&f),
   Some(&decision),Some(&route),&owner,"okx","bn",&mut gate,
   PublicWsEnvelope{sequence:2,body:&a_new,..a},
   PublicWsEnvelope{sequence:2,body:&b_new,..b});
  assert!(second.is_ok());
  assert!(gate.ready("BTC"));
  assert_eq!(paper.portfolio.stats.entries,1);
 }
 #[test]
 fn r129_owned_public_ws_route_denies_forged_connection(){
  use crate::native_public_turnover_ws_r123_review::PublicWsEnvelope;
  use crate::native_public_turnover_ws_continuity_r124_review::WsTurnoverContinuityR124;
  use crate::native_public_ws_owner_topology_r126_review::{TickerSubscription,PublicTickerTopologyR126};
  let owner=crate::owner_index("BTC",3);
  let okx_url="wss://ws.okx.com:8443/ws/v5/public";
  let fut_url="wss://fstream.binance.com/market/ws/btcusdt@ticker";
  let rows=vec![
   TickerSubscription{connection_id:"a".into(),owner,venue:Venue::OkxSpot,
     base:"BTC".into(),url:okx_url.into()},
   TickerSubscription{connection_id:"b".into(),owner,venue:Venue::BinanceFutures,
     base:"BTC".into(),url:fut_url.into()},
  ];
  let topology=PublicTickerTopologyR126::new(owner,&rows).unwrap();
  let mut guard=WsTurnoverContinuityR124::default();
  let mut paper=SupervisedStrategyPaper::new(fixture_cfg(),WALL).unwrap();
  let t=40_000_000_000;
  let (s,f)=entry_books(t,100.0);
  let route=entry_route(t);
  let decision=suggested("BTC",&route,&s,&f,t);
  let okx=format!(r#"{{"arg":{{"channel":"tickers","instId":"BTC-USDT"}},"data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"200000","ts":"{}"}}]}}"#,WALL-1000);
  let bn=format!(r#"{{"e":"24hrTicker","E":{},"s":"BTCUSDT","q":"200000","O":{},"C":{},"st":1}}"#,WALL-1000,WALL-86_401_000,WALL-1000);
  let a=PublicWsEnvelope{venue:Venue::OkxSpot,url:okx_url,
   base:"BTC",connection_epoch:1,sequence:1,received_wall_ms:WALL,body:&okx};
  let b=PublicWsEnvelope{venue:Venue::BinanceFutures,url:fut_url,
   base:"BTC",connection_epoch:1,sequence:1,received_wall_ms:WALL,body:&bn};
  let wrong=paper.on_frame_review_with_owned_ws_tickers(
    "BTC",Venue::OkxSpot,t,WALL,Some(&s),Some(&f),
    Some(&decision),Some(&route),&topology,"wrong","b",&mut guard,a,b);
  assert!(matches!(wrong,Err(StrategyFrameError::InvalidFrame)));
  assert_eq!(paper.portfolio.stats.entries,0);
  assert_eq!(guard.tracked(),0);
  paper.on_frame_review_with_owned_ws_tickers(
    "BTC",Venue::OkxSpot,t,WALL,Some(&s),Some(&f),
    Some(&decision),Some(&route),&topology,"a","b",&mut guard,a,b).unwrap();
  assert_eq!(paper.portfolio.stats.entries,1);
 }
 #[test]
 fn r124_ws_tickers_model_entry_does_not_block_owned_hedge(){
  use crate::native_public_turnover_ws_r123_review::PublicWsEnvelope;
  use crate::native_public_turnover_ws_continuity_r124_review::WsTurnoverContinuityR124;
  let mut guard=WsTurnoverContinuityR124::default();
  let mut paper=SupervisedStrategyPaper::new(fixture_cfg(),WALL).unwrap();
  let t=40_000_000_000;
  let (s,f)=entry_books(t,100.0);
  let route=entry_route(t);
  let decision=suggested("BTC",&route,&s,&f,t);
  let okx=format!(r#"{{"arg":{{"channel":"tickers","instId":"BTC-USDT"}},"data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"200000","ts":"{}"}}]}}"#,WALL-1000);
  let bn=format!(r#"{{"e":"24hrTicker","E":{},"s":"BTCUSDT","q":"200000","O":{},"C":{},"st":1}}"#,WALL-1000,WALL-86_401_000,WALL-1000);
  let a=PublicWsEnvelope{venue:Venue::OkxSpot,url:"wss://ws.okx.com:8443/ws/v5/public",
   base:"BTC",connection_epoch:1,sequence:1,received_wall_ms:WALL,body:&okx};
  let b=PublicWsEnvelope{venue:Venue::BinanceFutures,
   url:"wss://fstream.binance.com/market/ws/btcusdt@ticker",
   base:"BTC",connection_epoch:1,sequence:1,received_wall_ms:WALL,body:&bn};
  paper.on_frame_review_with_ws_tickers("BTC",Venue::OkxSpot,t,WALL,
    Some(&s),Some(&f),Some(&decision),Some(&route),&mut guard,a,b).unwrap();
  assert_eq!(paper.portfolio.stats.entries,1);
  let (s,f)=entry_books(t+1_000_000,100.0);
  paper.on_frame_review_with_ws_tickers("BTC",Venue::BinanceFutures,t+1_000_000,WALL,
    Some(&s),Some(&f),None,None,&mut guard,PublicWsEnvelope{body:"bad",..a},b).unwrap();
  assert_eq!(paper.portfolio.stats.futures_hedges,1);
 }
 #[test]
 fn r122_continuity_gate_and_existing_hedge_are_independent(){
  use crate::native_public_turnover_source_review::{PublicTickerSource,OKX_SPOT_TICKER,BINANCE_USDM_24H};
  use crate::native_public_turnover_continuity_r122_review::PublicTurnoverContinuityR122;
  let mut gate=PublicTurnoverContinuityR122::default();
  let mut paper=SupervisedStrategyPaper::new(fixture_cfg(),WALL).unwrap();
  let at=40_000_000_000;
  let (s,f)=entry_books(at,100.0);
  let route=entry_route(at);
  let signal=suggested("BTC",&route,&s,&f,at);
  let okx_body=format!(r#"{{"code":"0","data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"200000","ts":"{}"}}]}}"#,WALL-1000);
  let binance_body=format!(r#"{{"symbol":"BTCUSDT","quoteVolume":"200000","openTime":{},"closeTime":{}}}"#,WALL-86_401_000,WALL-1000);
  let okx=PublicTickerSource{venue:Venue::OkxSpot,endpoint:OKX_SPOT_TICKER,
    symbol:"BTC-USDT",generation:1,received_wall_ms:WALL,json:&okx_body};
  let binance=PublicTickerSource{venue:Venue::BinanceFutures,endpoint:BINANCE_USDM_24H,
    symbol:"BTCUSDT",generation:1,received_wall_ms:WALL,json:&binance_body};
  paper.on_frame_review_with_continuity_tickers("BTC",Venue::OkxSpot,at,WALL,
    Some(&s),Some(&f),Some(&signal),Some(&route),&mut gate,okx,binance).unwrap();
  assert_eq!(gate.observed_bases(),1);
  assert_eq!(paper.portfolio.stats.entries,1);
  let next=at+1_000_000;
  let (s,f)=entry_books(next,100.0);
  let invalid=PublicTickerSource{json:"invalid",..okx};
  paper.on_frame_review_with_continuity_tickers("BTC",Venue::BinanceFutures,next,WALL,
    Some(&s),Some(&f),None,None,&mut gate,invalid,binance).unwrap();
  assert_eq!(paper.portfolio.stats.futures_hedges,1);
 }
 #[test]
 fn r117_source_shaped_ticker_path_and_owned_hedge_are_independent(){
  use crate::native_public_turnover_source_review::{PublicTickerSource,OKX_SPOT_TICKER,BINANCE_USDM_24H};
  let mut paper=SupervisedStrategyPaper::new(fixture_cfg(),WALL).unwrap();
  let at=40_000_000_000;
  let (s,f)=entry_books(at,100.0);
  let route=entry_route(at);
  let signal=suggested("BTC",&route,&s,&f,at);
  let okx_body=format!(r#"{{"code":"0","data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"200000","ts":"{}"}}]}}"#,WALL-1000);
  let binance_body=format!(r#"{{"symbol":"BTCUSDT","quoteVolume":"200000","openTime":{},"closeTime":{}}}"#,WALL-86_401_000,WALL-1000);
  let okx=PublicTickerSource{venue:Venue::OkxSpot,endpoint:OKX_SPOT_TICKER,
    symbol:"BTC-USDT",generation:1,received_wall_ms:WALL,json:&okx_body};
  let binance=PublicTickerSource{venue:Venue::BinanceFutures,endpoint:BINANCE_USDM_24H,
    symbol:"BTCUSDT",generation:1,received_wall_ms:WALL,json:&binance_body};
  paper.on_frame_review_with_source_tickers("BTC",Venue::OkxSpot,at,WALL,
    Some(&s),Some(&f),Some(&signal),Some(&route),okx,binance).unwrap();
  assert_eq!(paper.portfolio.stats.entries,1);
  let invalid=PublicTickerSource{json:"invalid",..okx};
  let next=at+1_000_000;
  let (s,f)=entry_books(next,100.0);
  paper.on_frame_review_with_source_tickers("BTC",Venue::BinanceFutures,next,WALL,
    Some(&s),Some(&f),None,None,invalid,binance).unwrap();
  assert_eq!(paper.portfolio.stats.futures_hedges,1);
 }
 #[test]
 fn p106_r109_verified_entry_and_existing_hedge_are_independent(){
  let mut session=SupervisedStrategyPaper::new(fixture_cfg(),WALL).unwrap();
  let t=40_000_000_000;
  let (s,f)=entry_books(t,100.0);
  let route=entry_route(t);
  let hint=suggested("BTC",&route,&s,&f,t);
  assert_eq!(hint.reason,"EXECUTABLE_OPPORTUNITY");
  let result=session.on_frame_review_with_market("BTC",Venue::OkxSpot,t,WALL,
    Some(&s),Some(&f),Some(&hint),Some(&route),Some(&entry_market("BTC"))).unwrap();
  assert!(result.permitted_paper_legacy_model_update);
  assert_eq!(session.portfolio.stats.entries,1);
  assert!(matches!(session.portfolio.positions["BTC"].phase,
    PaperPhase::AwaitingHedge(_)));
  // Already opened virtual Spot must be allowed to hedge without a new
  // pre-approved entry candidate or caller-supplied decision.
  let next=t+1_000_000;
  let (s,f)=entry_books(next,100.0);
  session.on_frame_review("BTC",Venue::BinanceFutures,next,WALL,
    Some(&s),Some(&f),None,None).unwrap();
  assert_eq!(session.portfolio.stats.futures_hedges,1);
  assert!(matches!(session.portfolio.positions["BTC"].phase,PaperPhase::Hedged(_)));
 }
 #[test]
 fn p106_r109_spoofed_entry_cannot_bypass_yaml_blacklist_price_or_quantity(){
  let t=40_000_000_000;
  let route=entry_route(t);
  let (s,f)=entry_books(t,100.0);
  let mut fake_policy=crate::native_paper_config_bridge::
    bind_public_paper_decision_config(&fixture_cfg()).unwrap();
  fake_policy.blacklist.clear();
  let bypass=crate::decision::evaluate(
    "ONE",&route,Some(&s),Some(&f),t,WALL,&fake_policy);
  assert_eq!(bypass.reason,"EXECUTABLE_OPPORTUNITY");
  let mut session=SupervisedStrategyPaper::new(fixture_cfg(),WALL).unwrap();
  assert_eq!(session.on_frame_review("ONE",Venue::OkxSpot,t,WALL,
    Some(&s),Some(&f),Some(&bypass),Some(&route)).err(),
    Some(StrategyFrameError::InvalidFrame));
  assert_eq!(session.portfolio.stats.entries,0);

  let (low_s,low_f)=entry_books(t,0.02);
  let mut fake_policy=crate::native_paper_config_bridge::
    bind_public_paper_decision_config(&fixture_cfg()).unwrap();
  fake_policy.min_price=0.001;
  let bypass=crate::decision::evaluate(
    "BTC",&route,Some(&low_s),Some(&low_f),t,WALL,&fake_policy);
  assert_eq!(bypass.reason,"EXECUTABLE_OPPORTUNITY");
  assert_eq!(session.on_frame_review("BTC",Venue::OkxSpot,t,WALL,
    Some(&low_s),Some(&low_f),Some(&bypass),Some(&route)).err(),
    Some(StrategyFrameError::InvalidFrame));
  let mut forged=suggested("BTC",&route,&s,&f,t);
  forged.quantity=Some(0.2);
  assert_eq!(session.on_frame_review("BTC",Venue::OkxSpot,t,WALL,
    Some(&s),Some(&f),Some(&forged),Some(&route)).err(),
    Some(StrategyFrameError::InvalidFrame));
  assert!(session.portfolio.positions.is_empty());
 }
 #[test]
 fn p106_r109_existing_blacklisted_position_can_still_reduce_exposure(){
  let original=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  let edited=original
    .replacen("paper_max_trades_per_day: 3000",
             "paper_max_trades_per_day: 500",1)
    .replacen("- DASH","- X\n- DASH",1)
    .replacen("scale_in_hints:\n  enabled: true",
              "scale_in_hints:\n  enabled: false",1)
    .replacen("analytics:\n  enabled: true",
              "analytics:\n  enabled: false",1);
  assert_ne!(edited,original);
  let cfg=NativeTradingConfig::parse(&edited).unwrap();
  let mut session=SupervisedStrategyPaper::new(cfg,WALL).unwrap();
  session.portfolio=new_session().portfolio;
  let now=40_000_000_000;
  let e=session.on_frame_review("X",Venue::BinanceFutures,now,WALL,
    Some(&book(now,100.0,100.0)),
    Some(&book(now,99.9,100.0)),None,None).unwrap();
  assert!(matches!(e.decision,Some(CloseDecision::CloseNormal)));
  assert!(matches!(session.portfolio.positions["X"].phase,
    PaperPhase::AwaitingSpotExit{..}));
 }
 #[test]
 fn r110_missing_and_incorrect_public_volume_denies_new_entry(){
  let t=40_000_000_000;
  let route=entry_route(t);
  let (s,f)=entry_books(t,100.0);
  let proposed=suggested("BTC",&route,&s,&f,t);
  assert_eq!(proposed.reason,"EXECUTABLE_OPPORTUNITY");
  let mut paper=SupervisedStrategyPaper::new(fixture_cfg(),WALL).unwrap();
  assert_eq!(paper.on_frame_review("BTC",Venue::OkxSpot,t,WALL,
   Some(&s),Some(&f),Some(&proposed),Some(&route)).err(),
   Some(StrategyFrameError::InvalidFrame));
  for (spot,futures,at,base) in [
   (149_999.0,200_000.0,WALL,"BTC"),
   (200_000.0,149_999.0,WALL,"BTC"),
   (200_000.0,200_000.0,WALL-60_001,"BTC"),
   (200_000.0,200_000.0,WALL+1,"BTC"),
   (200_000.0,200_000.0,WALL,"ETH"),
  ]{
   let market=PublicEntryMarketSnapshot{base:base.into(),
    spot_volume_24h_quote:spot,futures_volume_24h_quote:futures,
    observed_wall_ms:at};
   assert_eq!(paper.on_frame_review_with_market("BTC",Venue::OkxSpot,t,WALL,
     Some(&s),Some(&f),Some(&proposed),Some(&route),Some(&market)).err(),
     Some(StrategyFrameError::InvalidFrame));
  }
  assert_eq!(paper.portfolio.stats.entries,0);
 }
 #[test]
 fn r110_sanity_spread_and_exact_volume_bounds(){
  let gate=PublicMarketEntryBounds::compile(&fixture_cfg()).unwrap();
  let mut m=entry_market("BTC");
  m.spot_volume_24h_quote=150_000.0;
  m.futures_volume_24h_quote=150_000.0;
  assert!(gate.admits("BTC",100.0,101.0,WALL,Some(&m)));
  assert!(!gate.admits("BTC",100.0,201.0,WALL,Some(&m)));
  assert!(!gate.admits("BTC",100.0,f64::NAN,WALL,Some(&m)));
  m.spot_volume_24h_quote=f64::NAN;
  assert!(!gate.admits("BTC",100.0,101.0,WALL,Some(&m)));
 }
 #[test]
 fn r112_cooldown_is_bound_to_legacy_yaml_and_replay(){
  let src=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  let yaml=src.replacen("paper_max_trades_per_day: 3000",
    "paper_max_trades_per_day: 500",1)
   .replacen("symbol_after_close_sec: 120","symbol_after_close_sec: 45",1)
   .replacen("symbol_cooldown_sec: 120","symbol_cooldown_sec: 30",1)
   .replacen("scale_in_hints:\n  enabled: true","scale_in_hints:\n  enabled: false",1)
   .replacen("analytics:\n  enabled: true","analytics:\n  enabled: false",1);
  assert_ne!(yaml,src);
  let cfg=NativeTradingConfig::parse(&yaml).unwrap();
  let paper=SupervisedStrategyPaper::new(cfg,WALL).unwrap();
  assert_eq!(paper.portfolio.reentry_cooldown_ns,45_000_000_000);
  assert_eq!(paper.portfolio.scan_cooldown_ns,30_000_000_000);
  let mut tampered=paper.replay_image().unwrap();
  tampered.portfolio.reentry_cooldown_ns=30_000_000_000;
  let mut tampered_scan=paper.replay_image().unwrap();
  tampered_scan.portfolio.scan_cooldown_ns=1_000_000_000;
  assert!(matches!(SupervisedStrategyPaper::from_replay_image(
   NativeTradingConfig::parse(&yaml).unwrap(),WALL,tampered_scan),
   Err(StrategyFrameError::Config)));
  assert!(matches!(SupervisedStrategyPaper::from_replay_image(
   NativeTradingConfig::parse(&yaml).unwrap(),WALL,tampered),
   Err(StrategyFrameError::Config)));
 }
 #[test]
 fn r111_forged_economic_fields_denied(){
  let t=40_000_000_000;
  let route=entry_route(t);
  let (s,f)=entry_books(t,100.0);
  let mut proposal=suggested("BTC",&route,&s,&f,t);
  assert_eq!(proposal.reason,"EXECUTABLE_OPPORTUNITY");
  proposal.spot_notional_usdt=Some(999.0);
  let mut paper=SupervisedStrategyPaper::new(fixture_cfg(),WALL).unwrap();
  assert_eq!(paper.on_frame_review_with_market("BTC",Venue::OkxSpot,t,WALL,
   Some(&s),Some(&f),Some(&proposal),Some(&route),Some(&entry_market("BTC"))).err(),
   Some(StrategyFrameError::InvalidFrame));
  assert_eq!(paper.portfolio.stats.entries,0);
 }
 #[test]
 fn p106_r101_paper_sequence_wait_hold_then_modeled_normal(){
  let mut session=new_session();
  // At 11sec age = 1sec; must not trigger the compiled 15sec rule.
  let now1=11_000_000_000;
  let a=session.on_frame_review("X",Venue::BinanceFutures,
    now1,WALL,Some(&book(now1,100.0,100.0)),
    Some(&book(now1,99.8,100.0)),None,None).unwrap();
  assert_eq!(a.decision,Some(CloseDecision::Wait));
  assert!(!a.permitted_paper_legacy_model_update);
  assert!(matches!(session.portfolio.positions["X"].phase,PaperPhase::Hedged(_)));

  let now2=30_000_000_000;
  // Current spread improves from -20 bps to -10 bps, velocity positive;
  // fresh executable close still equals 0 bps -> HOLD.
  let b=session.on_frame_review("X",Venue::BinanceFutures,
    now2,WALL,Some(&book(now2,100.0,100.0)),
    Some(&book(now2,99.9,100.0)),None,None).unwrap();
  assert_eq!(b.decision,Some(CloseDecision::Hold));
  assert!(!b.permitted_paper_legacy_model_update);
  assert!(matches!(session.portfolio.positions["X"].phase,PaperPhase::Hedged(_)));

  let now3=60_000_000_000;
  let c=session.on_frame_review("X",Venue::BinanceFutures,
    now3,WALL,Some(&book(now3,100.0,100.0)),
    Some(&book(now3,99.8,100.0)),None,None).unwrap();
  assert_eq!(c.decision,Some(CloseDecision::CloseNormal));
  assert!(c.permitted_paper_legacy_model_update);
  assert!(matches!(session.portfolio.positions["X"].phase,
   PaperPhase::AwaitingSpotExit{..}));
  // No real fill proven; modeled leg still requires NEXT OKX spot observation.
  assert_eq!(session.portfolio.stats.closed,0);
 }
 #[test]
 fn p106_r101_emergency_requires_manual_not_profits(){
  let mut session=new_session();
  let now=40_000_000_000;
  let e=session.on_frame_review("X",Venue::BinanceFutures,
    now,WALL,Some(&book(now,100.0,100.0)),
    Some(&book(now,99.4,100.0)),None,None);
  assert_eq!(e.err(),Some(StrategyFrameError::ManualEmergency));
  assert!(matches!(session.portfolio.positions["X"].phase,PaperPhase::Hedged(_)));
  assert_eq!(session.portfolio.stats.closed,0);
  // A later apparently profitable public frame cannot silently resume.
  let later=now+10_000_000_000;
  assert_eq!(session.on_frame_review("X",Venue::BinanceFutures,
    later,WALL,Some(&book(later,100.0,100.0)),
    Some(&book(later,99.9,100.0)),None,None).err(),
   Some(StrategyFrameError::ManualEmergency));
  assert!(matches!(session.portfolio.positions["X"].phase,PaperPhase::Hedged(_)));
 }
 #[test]
 fn p106_r101_exit_vwap_full_size_prevents_best_quote_false_close(){
  let mut session=new_session();
  let now=40_000_000_000;
  let mut spot=book(now,100.0,100.0);
  spot.bids=vec![(100.0,0.05),(99.0,1.0)];
  let mut fut=book(now,99.9,100.0);
  fut.asks=vec![(100.0,0.05),(101.0,1.0)];
  let e=session.on_frame_review("X",Venue::BinanceFutures,
    now,WALL,Some(&spot),Some(&fut),None,None).unwrap();
  assert_eq!(e.decision,Some(CloseDecision::Wait));
  assert!(!e.permitted_paper_legacy_model_update);
  assert!(matches!(session.portfolio.positions["X"].phase,PaperPhase::Hedged(_)));
  // Full hedge quantity must be known before any modeled close decision.
  let mut mismatch=new_session();
  if let PaperPhase::Hedged(ref mut p)=mismatch.portfolio.positions.get_mut("X").unwrap().phase{
   p.futures_short_base=Some(0.05);
  }
  assert_eq!(mismatch.on_frame_review("X",Venue::BinanceFutures,
    now,WALL,Some(&spot),Some(&fut),None,None).err(),
    Some(StrategyFrameError::ManualUnknown));
 }
 #[test]
 fn a5_entry_and_market_bounds_cannot_drift_after_source_admission(){
  for scenario in 0..11{
   let mut paper=SupervisedStrategyPaper::new(fixture_cfg(),WALL).unwrap();
   match scenario{
    0=>paper.entry_policy.max_age_ms+=1,
    1=>paper.entry_policy.metadata_ttl_ms+=1,
    2=>paper.entry_policy.base_size+=1.0,
    3=>paper.entry_policy.risk_cap+=1.0,
    4=>paper.entry_policy.cost+=1.0,
    5=>paper.entry_policy.required_net+=1.0,
    6=>paper.entry_policy.min_price+=0.01,
    7=>paper.entry_policy.max_price-=1.0,
    8=>{paper.entry_policy.blacklist.insert("BTC".into());},
    9=>paper.market_bounds.min_turnover_quote+=1.0,
    _=>paper.market_bounds.max_abs_spread_bps-=1.0,
   }
   assert!(paper.effective_research_config_report().is_err(),"scenario {scenario}");
   assert!(paper.replay_image().is_err(),"scenario {scenario}");
   let before=paper.portfolio.stats.entries;
   assert_eq!(paper.on_frame_review_with_market(
     "BTC",Venue::OkxSpot,40_000_000_000,WALL,None,None,None,None,None
   ).err(),Some(StrategyFrameError::Config),"scenario {scenario}");
   assert_eq!(paper.portfolio.stats.entries,before);
  }
 }
 #[test]
 fn a5_yaml_entry_and_public_market_filters_change_admission_not_just_reports(){
  let src=include_str!("../config/native_trading_operator_baseline_r64.yaml")
   .replacen("scale_in_hints:\n  enabled: true","scale_in_hints:\n  enabled: false",1)
   .replacen("analytics:\n  enabled: true","analytics:\n  enabled: false",1);
  let t=40_000_000_000;
  let (spot,fut)=entry_books(t,100.0);
  let route=entry_route(t);
  let cfg=NativeTradingConfig::parse(&src).unwrap();
  let accepted=crate::decision::evaluate("BTC",&route,
    Some(&spot),Some(&fut),t,WALL,
    &crate::native_paper_config_bridge::bind_public_paper_decision_config(&cfg).unwrap());
  assert_eq!(accepted.reason,"EXECUTABLE_OPPORTUNITY");
  let mut control=SupervisedStrategyPaper::new(cfg,WALL).unwrap();
  control.on_frame_review_with_market("BTC",Venue::OkxSpot,t,WALL,
    Some(&spot),Some(&fut),Some(&accepted),Some(&route),
    Some(&entry_market("BTC"))).unwrap();
  assert_eq!(control.portfolio.stats.entries,1);
  let changes=[
   ("entry_bps: 42.0","entry_bps: 100.0"),
   ("min_24h_volume_usdt: 150000","min_24h_volume_usdt: 300000"),
   ("min_price: 0.03","min_price: 101.0"),
   ("max_sanity_spread_bps: 10000","max_sanity_spread_bps: 70"),
  ];
  for (index,(old,new)) in changes.into_iter().enumerate(){
   let changed=src.replacen(old,new,1);
   assert_ne!(src,changed,"source replacement {index}");
   let cfg=NativeTradingConfig::parse(&changed).unwrap();
   let mut paper=SupervisedStrategyPaper::new(cfg,WALL).unwrap();
   let observed=paper.on_frame_review_with_market("BTC",Venue::OkxSpot,t,WALL,
     Some(&spot),Some(&fut),Some(&accepted),Some(&route),
     Some(&entry_market("BTC")));
   assert!(observed.is_err()||paper.portfolio.stats.entries==0,
      "source change {index} still admitted original executable entry");
   assert_eq!(paper.portfolio.stats.entries,0);
  }
 }
 #[test]
 fn a5_effective_research_source_report_exposes_all_entry_guard_values_without_runtime_claim(){
  let source=include_str!("../config/native_trading_operator_baseline_r64.yaml")
    .replacen("scale_in_hints:\n  enabled: true","scale_in_hints:\n  enabled: false",1)
    .replacen("analytics:\n  enabled: true","analytics:\n  enabled: false",1);
  let cfg=NativeTradingConfig::parse(&source).unwrap();
  let paper=SupervisedStrategyPaper::new(cfg.clone(),WALL).unwrap();
  let report=paper.effective_research_config_report().unwrap();
  assert_eq!(report["source_sha256"],cfg.sha256);
  assert_eq!(report["decision_entry_required_net_bps"],42.0);
  assert_eq!(report["decision_budget_quote"],10.0);
  assert_eq!(report["decision_max_book_age_ms"],800);
  assert_eq!(report["decision_metadata_ttl_ms"],43_200_000);
  assert_eq!(report["market_min_volume_quote_24h"],150_000.0);
  assert_eq!(report["market_max_abs_spread_bps"],10_000.0);
  assert_eq!(report["decision_blacklist_count"],11);
  assert_eq!(report["model_quote_budget"],10.0);
  assert_eq!(report["model_min_entry_net_bps"],42.0);
  assert_eq!(report["model_other_cost_bps"],24.03);
  assert_eq!(report["model_spot_buy_fee_base_bps"],0.0);
  assert_eq!(report["model_max_book_age_ns"],800_000_000);
  assert_eq!(report["compiled_legacy_default_daily_cap"],500);
  assert_eq!(report["review_source_quota_matches_compiled_legacy_default"],false);
  assert_eq!(report["public_ws_transport_attested"],false);
  assert_eq!(report["exchange_fills_attested"],false);
  assert_eq!(report["replay_image_is_durable"],false);
  assert_eq!(report["installed_runtime_source_bound"],false);
  assert!(report["deployed_binary_sha256"].is_null());
  assert_eq!(report["real_trading_authorized"],false);
 }
 #[test]
 fn a9_postparse_model_economics_cannot_drift_from_yaml(){
  for n in 0..6{
   let mut p=SupervisedStrategyPaper::new(fixture_cfg(),WALL).unwrap();
   match n{
    0=>p.portfolio.assumptions.quote_budget=9.0,
    1=>p.portfolio.assumptions.min_entry_net_bps=41.0,
    2=>p.portfolio.assumptions.other_cost_bps=0.0,
    3=>p.portfolio.assumptions.spot_buy_fee_base_bps=1.0,
    4=>p.portfolio.assumptions.max_book_age_ns=100_000_000,
    _=>p.portfolio.assumptions.take_profit_quote=Some(0.0),
   }
   assert!(p.effective_research_config_report().is_err(),"field {n}");
   assert!(p.replay_image().is_err(),"field {n}");
   assert_eq!(p.on_frame_review("BTC",Venue::OkxSpot,40_000_000_000,
     WALL,None,None,None,None).err(),Some(StrategyFrameError::Config));
  }
 }
 #[test]
 fn a7_temporal_yaml_changes_real_model_gate_and_cold_restore(){
  let raw=include_str!("../config/native_trading_operator_baseline_r64.yaml")
   .replacen("scale_in_hints:\n  enabled: true","scale_in_hints:\n  enabled: false",1)
   .replacen("analytics:\n  enabled: true","analytics:\n  enabled: false",1);
  let changed=raw.replacen("min_lifetime_sec: 15","min_lifetime_sec: 30",1)
   .replacen("exit_bps: 0.5","exit_bps: 1.0",1)
   .replacen("net_target_bps: 12.0","net_target_bps: 7.0",1)
   .replacen("symbol_cooldown_sec: 120","symbol_cooldown_sec: 60",1);
  assert_ne!(raw,changed);
  let a=SupervisedStrategyPaper::new(NativeTradingConfig::parse(&raw).unwrap(),WALL).unwrap();
  let cfg=NativeTradingConfig::parse(&changed).unwrap();
  let mut b=SupervisedStrategyPaper::new(cfg.clone(),WALL).unwrap();
  assert!(!a.portfolio.temporal_exit_eligible_review(35_000_000_000,0.75));
  assert!(b.portfolio.temporal_exit_eligible_review(35_000_000_000,0.75));
  assert!(!b.portfolio.temporal_exit_eligible_review(20_000_000_000,0.75));
  let report=b.effective_research_config_report().unwrap();
  assert_eq!(report["min_lifetime_ns"],30_000_000_000_u64);
  assert_eq!(report["exit_spread_bps"],1.0);
  assert_eq!(report["net_target_bps"],7.0);
  assert_eq!(report["scan_cooldown_ns"],60_000_000_000_u64);
  let image=b.replay_image().unwrap();
  let cold=SupervisedStrategyPaper::from_replay_image(cfg.clone(),WALL,image.clone()).unwrap();
  assert_eq!(cold.portfolio.min_lifetime_ns,30_000_000_000);
  assert!(SupervisedStrategyPaper::from_replay_image(
   NativeTradingConfig::parse(&raw).unwrap(),WALL,image).is_err());
  b.portfolio.net_target_bps=12.0;
  assert!(b.replay_image().is_err());
  assert!(b.effective_research_config_report().is_err());
  assert_eq!(b.on_frame_review("BTC",Venue::OkxSpot,40_000_000_000,WALL,
    None,None,None,None).err(),Some(StrategyFrameError::Config));
 }
 #[test]
 fn a4_source_3000_quota_reaches_modeled_entry_and_replay(){
  let source=include_str!("../config/native_trading_operator_baseline_r64.yaml")
   .replacen("scale_in_hints:\n  enabled: true","scale_in_hints:\n  enabled: false",1)
   .replacen("analytics:\n  enabled: true","analytics:\n  enabled: false",1);
  let cfg=NativeTradingConfig::parse(&source).unwrap();
  let mut paper=SupervisedStrategyPaper::new(cfg.clone(),WALL).unwrap();
  assert_eq!(paper.portfolio.max_daily_entries,3000);
  assert_eq!(paper.portfolio.max_concurrent_positions,10);
  let report=paper.effective_research_config_report().unwrap();
  assert_eq!(report["paper_max_daily_entries"],3000);
  assert_eq!(report["source_sha256"],cfg.sha256);
  assert_eq!(report["installed_runtime_source_bound"],false);
  let mut tampered=SupervisedStrategyPaper::new(cfg.clone(),WALL).unwrap();
  tampered.portfolio.max_daily_entries=500;
  assert!(tampered.effective_research_config_report().is_err());
  assert!(tampered.replay_image().is_err());
  assert_eq!(tampered.on_frame_review("BTC",Venue::OkxSpot,40_000_000_000,WALL,
    None,None,None,None).err(),Some(StrategyFrameError::Config));
  let t=40_000_000_000;
  let (s,f)=entry_books(t,100.0);
  let route=entry_route(t);
  let hint=suggested("BTC",&route,&s,&f,t);
  paper.on_frame_review_with_market("BTC",Venue::OkxSpot,t,WALL,
    Some(&s),Some(&f),Some(&hint),Some(&route),Some(&entry_market("BTC"))).unwrap();
  assert_eq!(paper.portfolio.stats.entries,1);
  // A4/B1 differential replay: identical books, source-only blacklist
  // change blocks the actual virtual entry, not merely a preview report.
  let blocked=source.replacen("- DASH","- BTC\n- DASH",1);
  assert_ne!(blocked,source);
  let other_cfg=NativeTradingConfig::parse(&blocked).unwrap();
  let other_decision=crate::native_paper_config_bridge::
    bind_public_paper_decision_config(&other_cfg).unwrap();
  let hint_blocked=crate::decision::evaluate(
    "BTC",&route,Some(&s),Some(&f),t,WALL,&other_decision);
  assert_ne!(hint_blocked.reason,"EXECUTABLE_OPPORTUNITY");
  let mut denied=SupervisedStrategyPaper::new(other_cfg,WALL).unwrap();
  denied.on_frame_review_with_market("BTC",Venue::OkxSpot,t,WALL,
    Some(&s),Some(&f),Some(&hint_blocked),Some(&route),
    Some(&entry_market("BTC"))).unwrap();
  assert_eq!(denied.portfolio.stats.entries,0);
  let image=paper.replay_image().unwrap();
  assert!(strategy_review_image_shape_valid(&image));
  let restored=SupervisedStrategyPaper::from_replay_image(cfg.clone(),WALL,image.clone()).unwrap();
  assert_eq!(restored.portfolio.max_daily_entries,3000);
  assert_eq!(restored.portfolio.stats.entries,1);
  let mut quota_tamper=image.clone();
  quota_tamper.portfolio.max_daily_entries=500;
  assert!(SupervisedStrategyPaper::from_replay_image(cfg,WALL,quota_tamper).is_err());
  let mut over=image;
  over.portfolio.stats.entries=3001;
  assert!(!strategy_review_image_shape_valid(&over));
 }
 #[test]
 fn p106_r101_unmodified_operator_yaml_quota_denied(){
  let cfg=NativeTradingConfig::parse(
   include_str!("../config/native_trading_operator_baseline_r64.yaml")
  ).unwrap();
  assert!(matches!(SupervisedStrategyPaper::new(cfg,WALL),
   Err(StrategyFrameError::Config)));
 }
 #[test]
 fn p106_r102_inmemory_replay_preserves_emergency_quarantine(){
  let mut session=new_session();
  let now=40_000_000_000;
  assert_eq!(session.on_frame_review("X",Venue::BinanceFutures,
    now,WALL,Some(&book(now,100.0,100.0)),
    Some(&book(now,99.4,100.0)),None,None).err(),
    Some(StrategyFrameError::ManualEmergency));
  let raw=serde_json::to_vec(&session.replay_image().unwrap()).unwrap();
  let image:StrategyReviewImage=serde_json::from_slice(&raw).unwrap();
  let mut restored=SupervisedStrategyPaper::from_replay_image(
    fixture_cfg(),WALL,image).unwrap();
  let later=now+10_000_000_000;
  assert_eq!(restored.on_frame_review("X",Venue::BinanceFutures,
    later,WALL,Some(&book(later,100.0,100.0)),
    Some(&book(later,99.9,100.0)),None,None).err(),
    Some(StrategyFrameError::ManualEmergency));
  assert_eq!(restored.portfolio.stats.closed,0);
 }
 #[test]
 fn p106_r102_inmemory_replay_denies_forged_quota_and_source(){
  let session=new_session();
  let image=session.replay_image().unwrap();
  let mut forged=image.clone();
  forged.config_sha256="0".repeat(64);
  assert!(matches!(SupervisedStrategyPaper::from_replay_image(
    fixture_cfg(),WALL,forged),Err(StrategyFrameError::Config)));
  let mut forged=image.clone();
  forged.real_trading_authorized=true;
  assert!(matches!(SupervisedStrategyPaper::from_replay_image(
    fixture_cfg(),WALL,forged),Err(StrategyFrameError::Config)));
  let mut forged=image.clone();
  forged.portfolio.stats.entries=501;
  assert!(matches!(SupervisedStrategyPaper::from_replay_image(
    fixture_cfg(),WALL,forged),Err(StrategyFrameError::Config)));
  let mut forged=image.clone();
  forged.previous_public_spreads.insert("X".into(),(f64::NAN,40.0));
  assert!(matches!(SupervisedStrategyPaper::from_replay_image(
    fixture_cfg(),WALL,forged),Err(StrategyFrameError::Config)));
  let mut forged=image;
  forged.portfolio.day_yek+=1;
  assert!(matches!(SupervisedStrategyPaper::from_replay_image(
    fixture_cfg(),WALL,forged),Err(StrategyFrameError::Config)));
 }
 #[test]
 fn p106_r104_poisoned_model_replay_refuses_forged_hedge_and_counters(){
  let session=new_session();
  let image=session.replay_image().unwrap();
  let denies=|image:StrategyReviewImage|
   matches!(SupervisedStrategyPaper::from_replay_image(fixture_cfg(),WALL,image),
    Err(StrategyFrameError::Config));
  let mut forged=image.clone();
  forged.portfolio.positions.get_mut("X").unwrap().phase=PaperPhase::Flat;
  assert!(denies(forged));
  let mut forged=image.clone();
  if let PaperPhase::Hedged(ref mut p)=forged.portfolio.positions.get_mut("X").unwrap().phase{
   p.futures_short_base=Some(0.05);
  }
  assert!(denies(forged));
  let mut forged=image.clone();
  if let PaperPhase::Hedged(ref mut p)=forged.portfolio.positions.get_mut("X").unwrap().phase{
   p.base="OTHER".into();
  }
  assert!(denies(forged));
  let mut forged=image.clone();
  if let PaperPhase::Hedged(ref mut p)=forged.portfolio.positions.get_mut("X").unwrap().phase{
   p.spot_paid_quote=f64::NAN;
  }
  assert!(denies(forged));
  let mut forged=image.clone();
  forged.portfolio.stats.entries=0;
  assert!(denies(forged));
  let mut forged=image.clone();
  forged.portfolio.stats.futures_hedges=0;
  forged.portfolio.stats.futures_covers=1;
  assert!(denies(forged));
  let mut forged=image;
  forged.portfolio.stats.closed=1;
  assert!(denies(forged));
 }
 #[test]
 fn r150_source_bound_frame_owns_virtual_entry_and_denies_missing_turnover(){
  // An operator-aligned research profile; no changes to original YAML.
  let raw=include_str!("../config/native_trading_operator_baseline_r64.yaml")
   .replacen("scale_in_hints:\n  enabled: true","scale_in_hints:\n  enabled: false",1)
   .replacen("analytics:\n  enabled: true","analytics:\n  enabled: false",1);
  let cfg=NativeTradingConfig::parse(&raw).unwrap();
  let t=40_000_000_000;
  let (spot,fut)=entry_books(t,100.0);
  let route=entry_route(t);
  let market=entry_market("BTC");
  let mut missing=SupervisedStrategyPaper::new(cfg.clone(),WALL).unwrap();
  assert_eq!(missing.on_source_bound_frame_review_with_market(
   "BTC",Venue::OkxSpot,t,WALL,Some(&spot),Some(&fut),Some(&route),None
  ).err(),Some(StrategyFrameError::InvalidFrame));
  assert_eq!(missing.portfolio.stats.entries,0);
  let mut source=SupervisedStrategyPaper::new(cfg.clone(),WALL).unwrap();
  let a=source.on_source_bound_frame_review_with_market(
   "BTC",Venue::OkxSpot,t,WALL,Some(&spot),Some(&fut),
   Some(&route),Some(&market)).unwrap();
  assert!(a.permitted_paper_legacy_model_update);
  assert_eq!(source.portfolio.stats.entries,1);
  assert_eq!(source.portfolio.max_daily_entries,3000);
  assert!(!a.proof_of_real_execution);
  assert_eq!(a.config_source_sha256,cfg.sha256);
  let report=source.effective_research_config_report().unwrap();
  assert_eq!(report["installed_runtime_source_bound"],false);
  let checkpoint=source.replay_image().unwrap();
  let cold=SupervisedStrategyPaper::from_replay_image(
   cfg.clone(),WALL,checkpoint.clone()).unwrap();
  assert_eq!(cold.portfolio.stats.entries,1);
  let stricter=raw.replacen("entry_bps: 42.0","entry_bps: 100.0",1);
  assert_ne!(stricter,raw);
  let stronger=NativeTradingConfig::parse(&stricter).unwrap();
  let mut denied=SupervisedStrategyPaper::new(stronger.clone(),WALL).unwrap();
  denied.on_source_bound_frame_review_with_market(
   "BTC",Venue::OkxSpot,t,WALL,Some(&spot),Some(&fut),
   Some(&route),Some(&market)).unwrap();
  assert_eq!(denied.portfolio.stats.entries,0);
  assert!(SupervisedStrategyPaper::from_replay_image(
   stronger,WALL,checkpoint).is_err());
 }
 #[test]
 fn r150_source_bound_frame_blacklist_changes_portfolio_not_just_report(){
  let raw=include_str!("../config/native_trading_operator_baseline_r64.yaml")
   .replacen("scale_in_hints:\n  enabled: true","scale_in_hints:\n  enabled: false",1)
   .replacen("analytics:\n  enabled: true","analytics:\n  enabled: false",1);
  let t=40_000_000_000;
  let (s,f)=entry_books(t,100.0);
  let route=entry_route(t);
  let market=entry_market("BTC");
  let blocked=raw.replacen("- DASH","- BTC\n- DASH",1);
  assert_ne!(blocked,raw);
  let mut paper=SupervisedStrategyPaper::new(
   NativeTradingConfig::parse(&blocked).unwrap(),WALL).unwrap();
  let result=paper.on_source_bound_frame_review_with_market(
    "BTC",Venue::OkxSpot,t,WALL,Some(&s),Some(&f),
    Some(&route),Some(&market)).unwrap();
  assert_eq!(paper.portfolio.stats.entries,0);
  assert!(!result.proof_of_real_execution);
 }
 #[test]
 fn p106_r101_bad_books_and_source_drift_denied(){
  let mut session=new_session();
  let now=40_000_000_000;
  assert_eq!(session.on_frame_review("X",Venue::BinanceFutures,
    now,WALL,Some(&book(now,101.0,100.0)),
    Some(&book(now,99.8,100.0)),None,None).err(),
    Some(StrategyFrameError::InvalidFrame));
  // The only mutable YAML is discarded before the first market frame.
  // A changed source must be caught at admission (never re-hashed per frame).
  let mut altered=fixture_cfg();
  altered.raw["velocity"]["hold_pos_bps_s"]=
   serde_yaml::Value::from(999.0);
  assert!(matches!(SupervisedStrategyPaper::new(altered,WALL),
    Err(StrategyFrameError::Config)));
 }
}
