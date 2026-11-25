//! R32D6C8A: native Rust YAML mirror + fail-closed, READ-ONLY risk previews.
//! Config is NOT connected to production dispatch. Neither legacy approval
//! nor this parser can override R32D6C1/R32D6C4 mutation/Hedged hard stops.
use serde_yaml::Value;
use serde::{Serialize,Deserialize};
use std::{collections::{BTreeMap,BTreeSet},fs,io::Read,path::Path};
use std::os::unix::fs::{MetadataExt,OpenOptionsExt};
use crate::authoritative_state::NativeEntryPlan;
use crate::fill_economics::{LegFillLedger,SCALE_12};

const ONE:u128=1_000_000_000_000_000_000;
const BPS_DIV:u128=10_000_000; // 1 milli-bps units
const MAX_BYTES:usize=256*1024;
const SECTIONS:[&str;26]=[
 "general","filters","spread","risk","real_trial","execution","limits",
 "telegram","logging","probe","entry_lock_ttl_sec","dynamic","dynamic_entry",
 "cooldown","telemetry","paths","scale_in_hints","adaptive_entry","velocity",
 "exit_safety","futures_emergency_stop","websocket","runtime_reconcile",
 "blacklist","analytics","fast_market_shadow"
];
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ConfigError{Io,Malformed,Schema,Policy,Notional,Unsupported}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativeTradingPolicy{
 pub quote_cap_e18:u128,pub nominal_order_e18:u128,
 pub general_max_positions:u64,pub real_max_positions:u64,
 pub general_daily_trades:u64,pub paper_daily_trades:u64,pub real_daily_trades:u64,
 pub paper_entries_enabled:bool,
 pub real_cooldown_sec:u64,pub entry_min_milli_bps:u128,pub fee_reserve_milli_bps:u128,
 pub daily_loss_cap_e18:u128,pub daily_okx_fee_cap_e18:u128,
 // P1-06: original Python market screening, now typed for Rust OFFLINE review.
 pub min_market_volume_24h_quote_e18:u128,
 pub min_market_price_quote_e18:u128,pub max_market_price_quote_e18:u128,
 pub max_market_abs_spread_milli_bps:u128,
 // Legacy Python retries: typed, bounded REVIEW only; never authorize resend.
 pub spot_limit_ticks:u64,pub spot_retry_attempts:u64,pub spot_retry_delay_ms:u64,
 pub futures_retry_attempts:u64,pub futures_retry_delay_ms:u64,
 // Original scanner/strategy top-N and WS leader cap; offline scope only.
 pub top_n_per_scan:usize,pub websocket_top_max:usize,pub websocket_enabled:bool,
 pub real_execution_authorized:bool,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct ReadOnlyRiskPreview{
 pub config_sha256:String,pub spot_notional_e18:u128,
 pub futures_notional_e18:u128,pub quote_with_reserved_fees_e18:u128,
 pub may_send_order:bool,pub may_promote_hedged:bool,
}
/// Conservative local market filter only; caller input is NOT exchange-verified.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativeMarketFilterPreview{
 pub passes_legacy_filters_locally:bool,
 pub reason:&'static str,
 pub exchange_source_authenticated:bool,
 pub may_send_or_cancel_order:bool,
}
/// The original Python retry knobs describe an ATTEMPT budget only.
/// It does not justify even the first exchange POST without a separate permit.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativeExecutionRetryBoundsReview{
 pub spot_limit_ticks:u64,
 pub spot_max_attempts:u64,pub spot_max_wait_before_last_ms:u64,
 pub futures_max_attempts:u64,pub futures_max_wait_before_last_ms:u64,
 pub known_exchange_outcome_authenticated:bool,
 pub may_resubmit_after_unknown_ack:bool,pub may_send_or_cancel_order:bool,
}
/// R149: separate legacy general trade switch & generic daily cap (300),
/// without confusing either with PAPER 3000/day or real_trial 0/day.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativeLegacyTradeAdmissionPreview{
 pub source_sha256:String,
 pub source_legacy_trade_enabled:bool,
 pub source_general_daily_trade_cap:u64,
 pub caller_known_daily_trades:Option<u64>,
 pub locally_reject_candidate:Option<bool>,
 pub account_day_complete_or_authenticated:bool,
 pub paper_quota_binding_verified:bool,
 pub real_launch_authorized:bool,
 pub may_send_or_cancel_order:bool,
 pub installed_paper_source_attested:bool,
}
/// R147 local input ages are unauthenticated, optional scalar examples.
/// None = UNKNOWN, never a successful freshness check.
#[derive(Clone,Debug,Default,PartialEq,Eq)]
pub struct NativeRealTrialClaimedAges{
 pub last_entry_age_sec:Option<u64>,
 pub preflight_age_sec:Option<u64>,
 pub mirror_lag_records:Option<u64>,
 pub kill_disengaged_age_sec:Option<u64>,
 pub kill_drill_age_sec:Option<u64>,
 pub telegram_delivery_age_sec:Option<u64>,
}
/// Eight original real_trial YAML knobs consumed ONLY by an immutable
/// read-only local diagnostic. The report never validates an actual exchange
/// account, key, file, durable cooldown, kill, or signed preflight.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativeRealTrialGuardWindowReview{
 pub source_sha256:String,
 pub source_trial_enabled:bool,
 pub source_bootstrap_first_trade:bool,
 pub source_entry_cooldown_sec:u64,
 pub source_preflight_max_age_sec:u64,
 pub source_max_mirror_lag_records:u64,
 pub source_kill_disengaged_max_age_sec:u64,
 pub source_kill_drill_max_age_sec:u64,
 pub source_telegram_delivery_max_age_sec:u64,
 pub local_cooldown_elapsed_claim:Option<bool>,
 pub local_preflight_fresh_claim:Option<bool>,
 pub local_mirror_lag_acceptable_claim:Option<bool>,
 pub local_kill_disengagement_fresh_claim:Option<bool>,
 pub local_kill_drill_fresh_claim:Option<bool>,
 pub local_telegram_delivery_fresh_claim:Option<bool>,
 pub has_exchange_authenticated_facts:bool,
 pub preflight_loaded_or_verified:bool,
 pub kill_switch_loaded_or_disengaged:bool,
 pub mirror_owner_state_authenticated:bool,
 pub cooldown_state_durably_verified:bool,
 pub telegram_delivery_authenticated:bool,
 pub may_launch_real:bool,
 pub may_modify_live_orders_or_paper:bool,
 pub installed_paper_source_attested:bool,
}
/// R146: original YAML financial ceilings applied to caller-supplied,
/// unauthenticated day aggregates. Neither a low known amount nor a missing
/// account-wide history can justify a new REAL order.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativeDailyFinancialSourceReview{
 pub original_config_sha256:String,
 pub source_daily_loss_cap_e18:u128,
 pub source_daily_okx_fee_cap_e18:u128,
 pub claimed_loss_e18:Option<u128>,
 pub claimed_okx_fees_e18:Option<u128>,
 pub claimed_loss_cap_reached:Option<bool>,
 pub claimed_fee_cap_reached:Option<bool>,
 pub entire_exchange_day_authenticated:bool,
 pub missing_entries_or_late_fees_excluded:bool,
 pub source_applied_to_installed_paper:bool,
 pub may_open_real:bool,
 pub may_mutate_paper_or_account:bool,
}
/// Original eight execution knobs, consumed only to produce a bounded,
/// read-only timing/tick contract. This does not justify attempts after a
/// rejected/UNKNOWN ACK; the entire timeline is configuration, NOT orders.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativeExecutionSourceTimingReview{
 pub config_sha256:String,
 pub spot_entry_mode:String,
 pub futures_entry_mode:String,
 pub unwind_mode:String,
 pub spot_limit_ticks:u64,
 pub spot_retry_offsets_ms:Vec<u64>,
 pub futures_retry_offsets_ms:Vec<u64>,
 pub original_exchange_acknowledged:bool,
 pub may_retry_after_unknown_ack:bool,
 pub may_send_or_cancel_order:bool,
 pub installed_paper_source_attested:bool,
}
/// Candidate rank and scanner WS leader subscriptions are independent sources.
/// They are caller-supplied LOCAL rankings; neither is a trusted trading signal.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativeCandidateAndWsPreview{
 pub top_candidate_bases:Vec<String>,
 pub ws_target_bases:Vec<String>,
 pub retained_open_position_bases:Vec<String>,
 pub exchange_source_authenticated:bool,
 pub may_update_live_ws_subscriptions:bool,pub may_trade:bool,
}
/// R32D6C8B1 immutable, durable READ-ONLY config snapshot, tied to the exact
/// initial intent. Neither this structure nor a validated config is a permit.
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeEntryPolicyBinding{
 pub config_sha256:String,
 pub safety_generation:u64,
 pub spot_limit_price_e12:u128,
 pub futures_reference_price_e12:u128,
 pub spot_notional_e18:u128,
 pub futures_notional_e18:u128,
 pub total_reserved_quote_e18:u128,
 pub user_quote_cap_e18:u128,
 pub config_order_quote_e18:u128,
 pub fee_reserve_milli_bps:u128,
 pub may_send_order:bool,
 pub may_promote_hedged:bool,
}
#[derive(Clone,Debug)]
pub struct NativeTradingConfig{
 pub raw:Value,pub policy:NativeTradingPolicy,pub sha256:String,
 pub mirrored_sections:usize,pub mirrored_leaf_paths:usize,
 pub migration_status:&'static str,
 // R93: parse-time identity; checked only on opt-in config binding, never hot path.
 parsed_raw_digest:[u8;32],parsed_policy:NativeTradingPolicy,parsed_sha256:String,
}
fn get<'a>(root:&'a Value,section:&str,key:&str)->Result<&'a Value,ConfigError>{
 root.get(section).and_then(|v|v.get(key)).ok_or(ConfigError::Schema)
}
fn integer(v:&Value)->Result<u64,ConfigError>{v.as_u64().ok_or(ConfigError::Malformed)}
fn yes(v:&Value)->Result<bool,ConfigError>{v.as_bool().ok_or(ConfigError::Malformed)}
fn raw_snapshot_digest(value:&Value)->Result<[u8;32],ConfigError>{
 let serialized=serde_json::to_vec(value).map_err(|_|ConfigError::Schema)?;
 Ok(openssl::sha::sha256(&serialized))
}
/// For high-stakes money/risk fields, serde_yaml::Number::to_string()
/// is NOT a financial lexical source: YAML floats may already have rounded
/// an input like 7.00000000000000000000001 to 7.0. Fail closed unless a
/// unique, ordinary top-level section / two-space field has an unquoted
/// decimal lexical token, then calculate solely from that exact text.
fn financial_yaml_literal<'a>(source:&'a str,section:&str,key:&str)
 ->Result<&'a str,ConfigError>{
 let section_header=format!("{section}:");
 let field_header=format!("  {key}:");
 let mut in_section=false;
 let mut found=None;
 for line in source.lines(){
    let line=line.trim_end_matches('\r');
    if !line.starts_with(' ')&&!line.starts_with('\t'){
       in_section=line.trim_end()==section_header;
       continue
    }
    if in_section{
       if let Some(rest)=line.strip_prefix(&field_header){
          if !rest.starts_with(' ')&& !rest.starts_with('\t'){
             return Err(ConfigError::Malformed)
          }
          let literal=rest.trim();
          let literal=literal.split_once(" #").map_or(literal,|(token,_)|token.trim_end());
          if found.is_some()||literal.is_empty()||literal.len()>96{
             return Err(ConfigError::Malformed)
          }
          found=Some(literal);
       }
    }
 }
 found.ok_or(ConfigError::Malformed)
}
fn financial_ceil_scaled(source:&str,raw:&Value,section:&str,key:&str,
 scale:u128)->Result<u128,ConfigError>{
 if !matches!(get(raw,section,key)?,Value::Number(_)){
    return Err(ConfigError::Malformed)
 }
 let repr=financial_yaml_literal(source,section,key)?;
 if repr.len()>96||repr.starts_with('-')||repr.starts_with('+'){
   return Err(ConfigError::Malformed)
 }
 let (whole,fraction)=repr.split_once('.').unwrap_or((repr,""));
 if whole.is_empty()||!whole.bytes().all(|b|b.is_ascii_digit())
   ||!fraction.bytes().all(|b|b.is_ascii_digit()){
    return Err(ConfigError::Malformed)
 }
 let integer=whole.parse::<u128>().map_err(|_|ConfigError::Malformed)?;
 if integer>100_000{return Err(ConfigError::Malformed)}
 let decimals=match scale{1000=>3,ONE=>18,_=>return Err(ConfigError::Malformed)};
 let mut fractional=0u128;
 let mut denominator=1u128;
 for &b in fraction.as_bytes().iter().take(decimals){
    fractional=fractional*10+(b-b'0') as u128;
    denominator*=10;
 }
 let extra_round_up=fraction.as_bytes().iter().skip(decimals).any(|&b|b!=b'0');
 let fractional=fractional.checked_mul(scale/denominator)
   .and_then(|n|n.checked_add(u128::from(extra_round_up)))
   .ok_or(ConfigError::Malformed)?;
 let total=integer.checked_mul(scale).and_then(|n|n.checked_add(fractional))
   .ok_or(ConfigError::Malformed)?;
 if total>100_000*scale{return Err(ConfigError::Malformed)}
 Ok(total)
}
fn ceil(n:u128,d:u128)->Result<u128,ConfigError>{
 n.checked_add(d-1).map(|n|n/d).ok_or(ConfigError::Notional)
}
fn notional(qty_e18:u128,price_e12:u128)->Result<u128,ConfigError>{
 if qty_e18==0||price_e12==0{return Err(ConfigError::Notional)}
 ceil(qty_e18.checked_mul(price_e12).ok_or(ConfigError::Notional)?,SCALE_12)
}
fn flatten(v:&Value,prefix:&str,out:&mut BTreeMap<String,String>)->Result<(),ConfigError>{
 match v{
  Value::Mapping(m)=>{
   if m.is_empty(){out.insert(prefix.into(),"mapping".into());return Ok(())}
   for (key,val) in m{
    let k=key.as_str().ok_or(ConfigError::Schema)?;
    if k.is_empty()||k.len()>96||k.contains('.') {return Err(ConfigError::Schema)}
    let name=if prefix.is_empty(){k.to_string()}else{format!("{prefix}.{k}")};
    flatten(val,&name,out)?;
   }
  },
  _=>{
   let typ=match v{
    Value::Null=>"null",Value::Bool(_)=>"boolean",
    Value::Number(n) if n.as_i64().is_some()||n.as_u64().is_some()=>"integer",
    Value::Number(_)=>"number",Value::String(_)=>"string",
    Value::Sequence(_)=>"sequence",_=>return Err(ConfigError::Schema),
   };
   out.insert(prefix.into(),typ.into());
  }
 }
 Ok(())
}
impl NativeEntryPolicyBinding{
 pub fn verify_exact_spot_fills(&self,plan:&NativeEntryPlan,ledger:&LegFillLedger)
  ->Result<u128,ConfigError>{
  self.validate_in_state(plan)?;
  if ledger.venue!="okx"||ledger.side!="buy"||ledger.symbol!=plan.spot_symbol
    ||!ledger.terminal||!ledger.cumulative_truth_present
    ||!ledger.fee_evidence_complete||ledger.fills.is_empty()
    ||ledger.filled_qty_atoms().checked_mul(1_000_000)
        .filter(|q|*q<=plan.planned_qty_atoms_e18).is_none(){
    return Err(ConfigError::Unsupported)
  }
  let mut spent_e12=0u128;
  for fill in ledger.fills.values(){
    let fee=fill.fee_cost_quote_atoms.ok_or(ConfigError::Unsupported)?;
    if fee<0{return Err(ConfigError::Unsupported)}
    let quote=ceil(fill.qty_atoms.checked_mul(fill.price_atoms)
       .ok_or(ConfigError::Notional)?,SCALE_12)?;
    spent_e12=spent_e12.checked_add(quote)
       .and_then(|v|v.checked_add(fee as u128))
       .ok_or(ConfigError::Notional)?;
  }
  let spent_e18=spent_e12.checked_mul(1_000_000).ok_or(ConfigError::Notional)?;
  if spent_e18>plan.order_quote_atoms_e18
     ||spent_e18>self.config_order_quote_e18||spent_e18>self.user_quote_cap_e18{
    return Err(ConfigError::Notional)
  }
  Ok(spent_e18)
 }
 pub fn validate_in_state(&self,plan:&NativeEntryPlan)->Result<(),ConfigError>{
  if self.config_sha256.len()!=64
    ||!self.config_sha256.bytes().all(|b|b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    ||self.safety_generation!=plan.safety_generation
    ||self.may_send_order||self.may_promote_hedged
    ||self.user_quote_cap_e18==0||self.user_quote_cap_e18>10*ONE
    ||self.config_order_quote_e18==0||self.config_order_quote_e18>self.user_quote_cap_e18
    ||plan.order_quote_atoms_e18==0
    ||plan.order_quote_atoms_e18>self.config_order_quote_e18
    ||self.fee_reserve_milli_bps>1_000_000{
   return Err(ConfigError::Policy)
  }
  let spot=notional(plan.planned_qty_atoms_e18,self.spot_limit_price_e12)?;
  let future=notional(plan.planned_qty_atoms_e18,self.futures_reference_price_e12)?;
  let gross=spot.max(future);
  let fee=ceil(gross.checked_mul(self.fee_reserve_milli_bps)
    .ok_or(ConfigError::Notional)?,BPS_DIV)?;
  let total=gross.checked_add(fee).ok_or(ConfigError::Notional)?;
  if spot!=self.spot_notional_e18||future!=self.futures_notional_e18
    ||total!=self.total_reserved_quote_e18||total>plan.order_quote_atoms_e18{
   return Err(ConfigError::Notional)
  }
  Ok(())
 }
}
impl NativeTradingConfig{
 pub fn parse(text:&str)->Result<Self,ConfigError>{
  if text.is_empty()||text.len()>MAX_BYTES{return Err(ConfigError::Schema)}
  let raw:Value=serde_yaml::from_str(text).map_err(|_|ConfigError::Malformed)?;
  let sections=raw.as_mapping().ok_or(ConfigError::Schema)?;
  if sections.len()!=SECTIONS.len()+1 ||SECTIONS.iter().any(|&k|raw.get(k).is_none())
    ||raw.get("native_runtime").is_none(){return Err(ConfigError::Schema)}
  let mut fields=BTreeMap::new();
  flatten(&raw,"",&mut fields)?;
  let required:BTreeMap<String,String>=serde_json::from_str(
    include_str!("../config/native_field_schema.json")).map_err(|_|ConfigError::Schema)?;
  if required!=fields{return Err(ConfigError::Schema)}
  // Old config flags are data, never authorization for Rust.
  let _=yes(get(&raw,"real_trial","operator_approved")?)?;
  let _=yes(get(&raw,"real_trial","autonomous_real_enabled")?)?;
  let _=yes(get(&raw,"general","trade_enabled")?)?;
  let paper_enabled=yes(get(&raw,"general","paper_entries_enabled")?)?;
  if integer(get(&raw,"native_runtime","config_schema_version")?)?!=1
    ||yes(get(&raw,"native_runtime","real_execution_authorized")?)?
    ||yes(get(&raw,"native_runtime","mutation_capability_enabled")?)?
    ||yes(get(&raw,"native_runtime","production_cutover_authorized")?)? {
   return Err(ConfigError::Policy)
  }
  let nominal=integer(get(&raw,"risk","futures_order_usdt")?)?;
  let general_pos=integer(get(&raw,"risk","max_open_positions")?)?;
  let real_pos=integer(get(&raw,"real_trial","max_open_positions")?)?;
  let real_day=integer(get(&raw,"real_trial","max_trades_per_day")?)?;
  let general_day=integer(get(&raw,"limits","max_trades_per_day")?)?;
  let paper_day=integer(get(&raw,"limits","paper_max_trades_per_day")?)?;
  let cooldown=integer(get(&raw,"real_trial","entry_cooldown_sec")?)?;
  let spread=financial_ceil_scaled(text,&raw,"spread","entry_bps",1000)?;
  // The EXACT raw lexical whole number guards 41.9999999999999999999,
  // which cannot be rounded up to grant a 42-bps minimum.
  let spread_literal=financial_yaml_literal(text,"spread","entry_bps")?;
  let spread_whole=spread_literal.split('.').next()
     .unwrap_or("").parse::<u128>().map_err(|_|ConfigError::Malformed)?;
  let daily_loss=financial_ceil_scaled(text,&raw,"limits","max_daily_loss_usdt",ONE)?;
  let daily_fees=financial_ceil_scaled(text,&raw,"limits","max_okx_fees_per_day_usdt",ONE)?;
  // P1-06 original strategy filters: retain exact minimum-price YAML token.
  // Fail closed on invalid source rather than silently applying Python fallback.
  let min_market_price=financial_ceil_scaled(text,&raw,"filters","min_price",ONE)?;
  let max_market_price=integer(get(&raw,"filters","max_price")?)? as u128*ONE;
  let min_market_volume=integer(get(&raw,"filters","min_24h_volume_usdt")?)? as u128*ONE;
  let max_market_abs_spread=integer(get(&raw,"filters","max_sanity_spread_bps")?)? as u128*1000;
  // Legacy strategy.py _place_order_with_retries: retry_n is the TOTAL
  // attempt count and retry_ms is the pause BETWEEN attempts. No POST here.
  let spot_limit_ticks=integer(get(&raw,"execution","spot_limit_ticks")?)?;
  let spot_retry_attempts=integer(get(&raw,"execution","spot_retry_n")?)?;
  let spot_retry_delay_ms=integer(get(&raw,"execution","spot_retry_ms")?)?;
  let futures_retry_attempts=integer(get(&raw,"execution","fut_retry_n")?)?;
  let futures_retry_delay_ms=integer(get(&raw,"execution","fut_retry_ms")?)?;
  // Legacy scanner and strategy have DISTINCT ordered sources: ranked
  // candidates for top-N, and scanner leaders for WS top_max subscriptions.
  let top_n_per_scan=integer(get(&raw,"limits","top_n_per_scan")?)?;
  let websocket_top_max=integer(get(&raw,"websocket","top_max")?)?;
  let websocket_enabled=yes(get(&raw,"websocket","enabled")?)?;
  let mut fees=0u128;
  for field in ["round_fees_bps","slippage_bps","buffer_bps","funding_bps"]{
    fees=fees.checked_add(financial_ceil_scaled(text,&raw,"spread",field,1000)?)
      .ok_or(ConfigError::Malformed)?;
  }
  if nominal==0||nominal>10||general_pos==0||real_pos!=1||real_day!=0
    ||general_day==0||paper_day==0||cooldown<10_800
    ||spread_whole<42||spread<42_000||daily_loss>10*ONE||daily_fees>7*ONE
    ||fees>1_000_000 // match NativeEntryPolicyBinding::validate_in_state
    ||min_market_price==0||max_market_price<min_market_price
    ||min_market_volume==0||max_market_abs_spread==0
    ||spot_limit_ticks>20
    ||!(1..=8).contains(&spot_retry_attempts)
    ||!(1..=8).contains(&futures_retry_attempts)
    ||!(50..=10_000).contains(&spot_retry_delay_ms)
    ||!(50..=10_000).contains(&futures_retry_delay_ms)
    ||(spot_retry_attempts-1)*spot_retry_delay_ms>30_000
    ||(futures_retry_attempts-1)*futures_retry_delay_ms>30_000
    ||!(1..=1_000).contains(&top_n_per_scan)
    ||!(1..=500).contains(&websocket_top_max)
    ||!yes(get(&raw,"real_trial","deny_on_unknown")?)?
    ||!yes(get(&raw,"real_trial","deny_on_untracked_assets")?)?
    ||get(&raw,"execution","spot_entry_mode")?.as_str()!=Some("aggressive_limit")
    ||get(&raw,"execution","fut_entry_mode")?.as_str()!=Some("market")
    ||get(&raw,"execution","unwind_mode")?.as_str()!=Some("market"){
   return Err(ConfigError::Policy)
  }
  let mut hash=String::with_capacity(64);
  for b in openssl::sha::sha256(text.as_bytes()){hash.push_str(&format!("{b:02x}"));}
  let policy=NativeTradingPolicy{
   quote_cap_e18:10*ONE,nominal_order_e18:nominal as u128*ONE,
   general_max_positions:general_pos,real_max_positions:real_pos,
   general_daily_trades:general_day,paper_daily_trades:paper_day,
   real_daily_trades:real_day,paper_entries_enabled:paper_enabled,
   real_cooldown_sec:cooldown,
   entry_min_milli_bps:spread,fee_reserve_milli_bps:fees,
   daily_loss_cap_e18:daily_loss,
   daily_okx_fee_cap_e18:daily_fees,
   min_market_volume_24h_quote_e18:min_market_volume,
   min_market_price_quote_e18:min_market_price,
   max_market_price_quote_e18:max_market_price,
   max_market_abs_spread_milli_bps:max_market_abs_spread,
   spot_limit_ticks,spot_retry_attempts,spot_retry_delay_ms,
   futures_retry_attempts,futures_retry_delay_ms,
   top_n_per_scan:top_n_per_scan as usize,
   websocket_top_max:websocket_top_max as usize,websocket_enabled,
   real_execution_authorized:false,
  };
  let parsed_raw_digest=raw_snapshot_digest(&raw)?;
  let parsed_policy=policy.clone();
  let parsed_sha256=hash.clone();
  Ok(Self{raw,policy,sha256:hash,mirrored_sections:SECTIONS.len(),
   mirrored_leaf_paths:fields.len()-4,
   migration_status:"MIRRORED_AND_VALIDATED_NOT_FULLY_WIRED",
   parsed_raw_digest,parsed_policy,parsed_sha256})
 }
 /// For an opt-in public PAPER review, reject all in-memory divergence from
 /// the originally parsed YAML and typed policy. This is NOT exchange auth.
 pub(crate) fn source_projection_unchanged(&self)->bool{
  self.sha256==self.parsed_sha256 && self.policy==self.parsed_policy
   &&raw_snapshot_digest(&self.raw).ok()==Some(self.parsed_raw_digest)
 }
 /// P1-06 deterministic RETRY SHAPE ONLY: duplicate order risk from UNKNOWN
 /// ACK cannot be solved by arithmetic. Historical Python attempt totals and
 /// wait-before-last are exposed without creating an execution API.
 pub fn inspect_execution_retry_bounds_read_only(
  &self,
 )->Result<NativeExecutionRetryBoundsReview,ConfigError>{
  if !self.source_projection_unchanged(){return Err(ConfigError::Policy)}
  let p=&self.policy;
  if p.real_execution_authorized||p.spot_limit_ticks>20
   ||!(1..=8).contains(&p.spot_retry_attempts)
   ||!(1..=8).contains(&p.futures_retry_attempts)
   ||!(50..=10_000).contains(&p.spot_retry_delay_ms)
   ||!(50..=10_000).contains(&p.futures_retry_delay_ms){
   return Err(ConfigError::Policy)
  }
  let spot_wait=(p.spot_retry_attempts-1)
   .checked_mul(p.spot_retry_delay_ms).ok_or(ConfigError::Policy)?;
  let futures_wait=(p.futures_retry_attempts-1)
   .checked_mul(p.futures_retry_delay_ms).ok_or(ConfigError::Policy)?;
  if spot_wait>30_000||futures_wait>30_000{return Err(ConfigError::Policy)}
  Ok(NativeExecutionRetryBoundsReview{
   spot_limit_ticks:p.spot_limit_ticks,
   spot_max_attempts:p.spot_retry_attempts,
   spot_max_wait_before_last_ms:spot_wait,
   futures_max_attempts:p.futures_retry_attempts,
   futures_max_wait_before_last_ms:futures_wait,
   known_exchange_outcome_authenticated:false,
   may_resubmit_after_unknown_ack:false,
   may_send_or_cancel_order:false,
  })
 }
 /// R149: two *historical* knobs affect a LOCAL candidate denial preview.
 /// They are NOT real grants and the generic 300/day cap is NOT PAPER's
 /// separate 3000/day or a venue-authenticated count. None stays unknown.
 pub fn inspect_legacy_general_entry_budget_readonly(
  &self,caller_known_daily_trades:Option<u64>
 )->Result<NativeLegacyTradeAdmissionPreview,ConfigError>{
  if !self.source_projection_unchanged()
    ||self.policy.real_execution_authorized||self.policy.real_daily_trades!=0{
   return Err(ConfigError::Policy)
  }
  let flag=yes(get(&self.raw,"general","trade_enabled")?)?;
  let daily=integer(get(&self.raw,"limits","max_trades_per_day")?)?;
  if daily==0||daily!=self.policy.general_daily_trades{
   return Err(ConfigError::Policy)
  }
  let locally_reject_candidate=if !flag{Some(true)}
    else{caller_known_daily_trades.map(|count|count>=daily)};
  Ok(NativeLegacyTradeAdmissionPreview{
   source_sha256:self.sha256.clone(),
   source_legacy_trade_enabled:flag,
   source_general_daily_trade_cap:daily,
   caller_known_daily_trades,
   locally_reject_candidate,
   account_day_complete_or_authenticated:false,
   paper_quota_binding_verified:false,
   real_launch_authorized:false,
   may_send_or_cancel_order:false,
   installed_paper_source_attested:false,
  })
 }
 /// R147 source->typed limits->untrusted examples only. Source identity
 /// remains bound to parse-time fingerprint. Freshness YES in any field is
 /// NOT an admission decision: real and network actions remain prohibited.
 pub fn inspect_real_trial_guard_windows_readonly(
   &self,claims:&NativeRealTrialClaimedAges,
 )->Result<NativeRealTrialGuardWindowReview,ConfigError>{
  if !self.source_projection_unchanged()
    ||self.policy.real_execution_authorized||self.policy.real_daily_trades!=0{
    return Err(ConfigError::Policy)
  }
  let trial_enabled=yes(get(&self.raw,"real_trial","enabled")?)?;
  let bootstrap=yes(get(&self.raw,"real_trial","bootstrap_first_trade")?)?;
  let cooldown=integer(get(&self.raw,"real_trial","entry_cooldown_sec")?)?;
  let preflight_age=integer(get(&self.raw,"real_trial","preflight_max_age_sec")?)?;
  let mirror_lag=integer(get(&self.raw,"real_trial","max_mirror_lag_records")?)?;
  let kill_disengaged=integer(get(&self.raw,"real_trial",
      "kill_switch_disengaged_max_age_sec")?)?;
  let kill_drill=integer(get(&self.raw,"real_trial",
      "kill_switch_drill_max_age_sec")?)?;
  let tg_delivery=integer(get(&self.raw,"real_trial",
      "telegram_delivery_max_age_sec")?)?;
  // Avoid accidental zero-age "fresh forever" or widened unbounded limits.
  // Historical originals are 10800, 3600, 32, 900, 2592000 and 3600.
  if cooldown!=self.policy.real_cooldown_sec||cooldown<10_800
    ||preflight_age==0||preflight_age>86_400
    ||mirror_lag==0||mirror_lag>1_000
    ||kill_disengaged==0||kill_disengaged>3_600
    ||kill_drill==0||kill_drill>2_592_000
    ||tg_delivery==0||tg_delivery>86_400{
    return Err(ConfigError::Policy)
  }
  Ok(NativeRealTrialGuardWindowReview{
   source_sha256:self.sha256.clone(),
   source_trial_enabled:trial_enabled,
   source_bootstrap_first_trade:bootstrap,
   source_entry_cooldown_sec:cooldown,
   source_preflight_max_age_sec:preflight_age,
   source_max_mirror_lag_records:mirror_lag,
   source_kill_disengaged_max_age_sec:kill_disengaged,
   source_kill_drill_max_age_sec:kill_drill,
   source_telegram_delivery_max_age_sec:tg_delivery,
   local_cooldown_elapsed_claim:claims.last_entry_age_sec.map(|age|age>=cooldown),
   local_preflight_fresh_claim:claims.preflight_age_sec.map(|age|age<=preflight_age),
   local_mirror_lag_acceptable_claim:claims.mirror_lag_records.map(|lag|lag<=mirror_lag),
   local_kill_disengagement_fresh_claim:claims.kill_disengaged_age_sec.map(
       |age|age<=kill_disengaged),
   local_kill_drill_fresh_claim:claims.kill_drill_age_sec.map(|age|age<=kill_drill),
   local_telegram_delivery_fresh_claim:claims.telegram_delivery_age_sec.map(
       |age|age<=tg_delivery),
   has_exchange_authenticated_facts:false,
   preflight_loaded_or_verified:false,
   kill_switch_loaded_or_disengaged:false,
   mirror_owner_state_authenticated:false,
   cooldown_state_durably_verified:false,
   telegram_delivery_authenticated:false,
   may_launch_real:false,
   may_modify_live_orders_or_paper:false,
   installed_paper_source_attested:false,
  })
 }
 /// P1-06 R146: a local financial threshold review only. Existing
 /// NativeTradingConfig::parse derives both ceilings from exact decimal YAML
 /// source tokens. Refuse untracked in-memory policy drift. Absence of a
 /// claimed aggregate stays UNKNOWN, never coerced into zero.
 pub fn inspect_financial_day_source_limits_without_io(
  &self,claimed_loss_e18:Option<u128>,
  claimed_okx_fees_e18:Option<u128>,
 )->Result<NativeDailyFinancialSourceReview,ConfigError>{
  if !self.source_projection_unchanged()
    ||self.policy.real_execution_authorized||self.policy.real_daily_trades!=0
    ||self.policy.daily_loss_cap_e18==0||self.policy.daily_okx_fee_cap_e18==0{
    return Err(ConfigError::Policy)
  }
  Ok(NativeDailyFinancialSourceReview{
    original_config_sha256:self.sha256.clone(),
    source_daily_loss_cap_e18:self.policy.daily_loss_cap_e18,
    source_daily_okx_fee_cap_e18:self.policy.daily_okx_fee_cap_e18,
    claimed_loss_e18,claimed_okx_fees_e18,
    claimed_loss_cap_reached:claimed_loss_e18
      .map(|known|known>=self.policy.daily_loss_cap_e18),
    claimed_fee_cap_reached:claimed_okx_fees_e18
      .map(|known|known>=self.policy.daily_okx_fee_cap_e18),
    entire_exchange_day_authenticated:false,
    missing_entries_or_late_fees_excluded:false,
    source_applied_to_installed_paper:false,
    may_open_real:false,
    may_mutate_paper_or_account:false,
  })
 }
 /// P1-06: show all eight original execution YAML knobs as a single
 /// deterministic, source-bound *review* schedule. Reuses the existing
 /// retry bound check, and does NOT execute even attempt zero.
 pub fn inspect_original_execution_timing_read_only(
  &self,
 )->Result<NativeExecutionSourceTimingReview,ConfigError>{
  let limits=self.inspect_execution_retry_bounds_read_only()?;
  let orig=get(&self.raw,"execution","spot_entry_mode")?
    .as_str().ok_or(ConfigError::Schema)?;
  let fut=get(&self.raw,"execution","fut_entry_mode")?
    .as_str().ok_or(ConfigError::Schema)?;
  let unwind=get(&self.raw,"execution","unwind_mode")?
    .as_str().ok_or(ConfigError::Schema)?;
  if orig!="aggressive_limit"||fut!="market"||unwind!="market"{
    return Err(ConfigError::Policy)
  }
  let offsets=|attempts:u64,interval:u64|->Result<Vec<u64>,ConfigError>{
    (0..attempts).map(|i|i.checked_mul(interval)
       .ok_or(ConfigError::Policy)).collect()
  };
  let spot=offsets(limits.spot_max_attempts,self.policy.spot_retry_delay_ms)?;
  let futures=offsets(limits.futures_max_attempts,self.policy.futures_retry_delay_ms)?;
  if spot.last().copied()!=Some(limits.spot_max_wait_before_last_ms)
    ||futures.last().copied()!=Some(limits.futures_max_wait_before_last_ms){
    return Err(ConfigError::Policy)
  }
  Ok(NativeExecutionSourceTimingReview{
    config_sha256:self.sha256.clone(),
    spot_entry_mode:orig.to_owned(),futures_entry_mode:fut.to_owned(),
    unwind_mode:unwind.to_owned(),spot_limit_ticks:limits.spot_limit_ticks,
    spot_retry_offsets_ms:spot,futures_retry_offsets_ms:futures,
    original_exchange_acknowledged:false,
    may_retry_after_unknown_ack:false,may_send_or_cancel_order:false,
    installed_paper_source_attested:false,
  })
 }
 /// Legacy scanner + strategy use different ranked lists: candidate top-N
 /// uses descending spread with stable ties, while WS targets use the first
 /// top_max scanner leaders PLUS every already open position. All inputs
 /// are unauthenticated; no network subscriptions, trading or mutations.
 pub fn inspect_candidate_and_ws_selection_read_only(
  &self,candidates:&[(&str,i64)],scanner_leaders:&[&str],
  open_position_bases:&[&str],
 )->Result<NativeCandidateAndWsPreview,ConfigError>{
  let p=&self.policy;
  if p.real_execution_authorized||!(1..=1000).contains(&p.top_n_per_scan)
   ||!(1..=500).contains(&p.websocket_top_max)
   ||candidates.len()>4096||scanner_leaders.len()>4096
   ||open_position_bases.len()>1000{
   return Err(ConfigError::Policy)
  }
  let valid=|base:&str|!base.is_empty()&&base.len()<=32
   &&base.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit());
  let mut candidate_ids=BTreeSet::new();
  let mut ranked=Vec::with_capacity(candidates.len());
  for (index,&(base,spread)) in candidates.iter().enumerate(){
   if !valid(base)||!candidate_ids.insert(base.to_owned())
    ||!(-20_000_000..=20_000_000).contains(&spread){
    return Err(ConfigError::Unsupported)
   }
   ranked.push((index,base,spread));
  }
  ranked.sort_by(|a,b|b.2.cmp(&a.2).then_with(||a.0.cmp(&b.0)));
  let top_candidate_bases=ranked.iter().take(p.top_n_per_scan)
   .map(|(_,base,_)|(*base).to_owned()).collect();
  // Independently preserve occupied owners even when not on scanner leaders.
  let mut seen_owners=BTreeSet::new();
  let mut retained_open_position_bases=Vec::new();
  for &base in open_position_bases{
   if !valid(base){return Err(ConfigError::Unsupported)}
   if seen_owners.insert(base.to_owned()){
    retained_open_position_bases.push(base.to_owned());
   }
  }
  let mut ws_target_bases=Vec::new();
  let mut seen_targets=BTreeSet::new();
  for &base in scanner_leaders{
   if !valid(base){return Err(ConfigError::Unsupported)}
  }
  if p.websocket_enabled{
   for &base in scanner_leaders.iter().take(p.websocket_top_max){
    if seen_targets.insert(base.to_owned()){
     ws_target_bases.push(base.to_owned());
    }
   }
   for base in &retained_open_position_bases{
    if seen_targets.insert(base.clone()){
     ws_target_bases.push(base.clone());
    }
   }
  }
  Ok(NativeCandidateAndWsPreview{
   top_candidate_bases,ws_target_bases,retained_open_position_bases,
   exchange_source_authenticated:false,
   may_update_live_ws_subscriptions:false,may_trade:false,
  })
 }
 /// P1-06: typed replica of original Python min/max price, 24h liquidity,
 /// and absolute sanity-spread screen, for OFFLINE read-only comparison.
 /// Caller-supplied observations are NOT authenticated or tied to book age.
 /// This is neither a trade permit nor a substitute for source provenance.
 pub fn inspect_market_filters_read_only(
  &self,spot_price_quote_e18:u128,volume_24h_quote_e18:u128,
  signed_spread_milli_bps:i128,
 )->Result<NativeMarketFilterPreview,ConfigError>{
  let magnitude=signed_spread_milli_bps.checked_abs()
    .ok_or(ConfigError::Unsupported)? as u128;
  let p=&self.policy;
  if spot_price_quote_e18==0||p.real_execution_authorized{
   return Err(ConfigError::Unsupported)
  }
  let reason=if spot_price_quote_e18<p.min_market_price_quote_e18{
   "BELOW_MIN_PRICE"
  }else if spot_price_quote_e18>p.max_market_price_quote_e18{
   "ABOVE_MAX_PRICE"
  }else if volume_24h_quote_e18<p.min_market_volume_24h_quote_e18{
   "BELOW_24H_VOLUME"
  }else if magnitude>p.max_market_abs_spread_milli_bps{
   "SANITY_SPREAD_EXCEEDED"
  }else{"LOCAL_FILTER_PASS_NOT_TRADE_AUTHORIZATION"};
  Ok(NativeMarketFilterPreview{
   passes_legacy_filters_locally:reason=="LOCAL_FILTER_PASS_NOT_TRADE_AUTHORIZATION",
   reason,exchange_source_authenticated:false,may_send_or_cancel_order:false,
  })
 }
 pub fn bind_entry(&self,plan:&NativeEntryPlan,spot_limit_e12:u128,
  futures_reference_e12:u128,safety_generation:u64
 )->Result<NativeEntryPolicyBinding,ConfigError>{
  let preview=self.preview_plan(plan,spot_limit_e12,futures_reference_e12,safety_generation)?;
  Ok(NativeEntryPolicyBinding{
   config_sha256:preview.config_sha256,
   safety_generation,spot_limit_price_e12:spot_limit_e12,
   futures_reference_price_e12:futures_reference_e12,
   spot_notional_e18:preview.spot_notional_e18,
   futures_notional_e18:preview.futures_notional_e18,
   total_reserved_quote_e18:preview.quote_with_reserved_fees_e18,
   user_quote_cap_e18:self.policy.quote_cap_e18,
   config_order_quote_e18:self.policy.nominal_order_e18,
   fee_reserve_milli_bps:self.policy.fee_reserve_milli_bps,
   may_send_order:false,may_promote_hedged:false,
  })
 }
 pub fn validate_binding(&self,plan:&NativeEntryPlan,b:&NativeEntryPolicyBinding
 )->Result<(),ConfigError>{
  let candidate=self.bind_entry(plan,b.spot_limit_price_e12,
   b.futures_reference_price_e12,b.safety_generation)?;
  if &candidate!=b{return Err(ConfigError::Policy)}
  Ok(())
 }
 pub fn load(path:&Path)->Result<Self,ConfigError>{
  // A path preflight + read_to_string follows an attacker-swapped symlink
  // after lstat. Open only a REGULAR file, using O_NOFOLLOW and a single
  // fd. A parent-directory swap is detected by comparing the final path
  // identity to the opened descriptor; metadata drift fails closed.
  let before=fs::symlink_metadata(path).map_err(|_|ConfigError::Io)?;
  if !before.file_type().is_file()||before.len()>MAX_BYTES as u64{
    return Err(ConfigError::Io)
  }
  let mut file=fs::OpenOptions::new().read(true)
    .custom_flags(libc::O_NOFOLLOW|libc::O_NONBLOCK|libc::O_CLOEXEC)
    .open(path).map_err(|_|ConfigError::Io)?;
  let opened=file.metadata().map_err(|_|ConfigError::Io)?;
  if !opened.is_file()||opened.dev()!=before.dev()
    ||opened.ino()!=before.ino()||opened.len()!=before.len(){
    return Err(ConfigError::Io)
  }
  let mut bytes=Vec::new();
  file.by_ref().take((MAX_BYTES as u64)+1).read_to_end(&mut bytes)
    .map_err(|_|ConfigError::Io)?;
  if bytes.len()>MAX_BYTES{return Err(ConfigError::Io)}
  let after_fd=file.metadata().map_err(|_|ConfigError::Io)?;
  let after_path=fs::symlink_metadata(path).map_err(|_|ConfigError::Io)?;
  if !after_path.file_type().is_file()||bytes.len() as u64!=opened.len()
    ||after_fd.dev()!=opened.dev()||after_fd.ino()!=opened.ino()
    ||after_path.dev()!=opened.dev()||after_path.ino()!=opened.ino()
    ||after_fd.len()!=opened.len()||after_path.len()!=opened.len()
    ||after_fd.mtime()!=opened.mtime()||after_fd.mtime_nsec()!=opened.mtime_nsec()
    ||after_fd.ctime()!=opened.ctime()||after_fd.ctime_nsec()!=opened.ctime_nsec()
    ||after_path.mtime()!=opened.mtime()||after_path.mtime_nsec()!=opened.mtime_nsec()
    ||after_path.ctime()!=opened.ctime()||after_path.ctime_nsec()!=opened.ctime_nsec(){
    return Err(ConfigError::Io)
  }
  let text=std::str::from_utf8(&bytes).map_err(|_|ConfigError::Malformed)?;
  Self::parse(text)
 }
 /// Safe mathematical admission preview ONLY. No network/order/Hedged permit.
 pub fn preview_plan(&self,plan:&NativeEntryPlan,spot_price_e12:u128,
  futures_price_e12:u128,current_safety_generation:u64
 )->Result<ReadOnlyRiskPreview,ConfigError>{
  if plan.safety_generation!=current_safety_generation
    ||self.policy.real_daily_trades!=0||self.policy.real_execution_authorized
    ||plan.order_quote_atoms_e18==0
    ||plan.order_quote_atoms_e18>self.policy.nominal_order_e18
    ||plan.order_quote_atoms_e18>self.policy.quote_cap_e18{
   return Err(ConfigError::Policy)
  }
  let s=notional(plan.planned_qty_atoms_e18,spot_price_e12)?;
  let f=notional(plan.planned_qty_atoms_e18,futures_price_e12)?;
  let gross=s.max(f);
  let fee=ceil(gross.checked_mul(self.policy.fee_reserve_milli_bps)
   .ok_or(ConfigError::Notional)?,BPS_DIV)?;
  let full=gross.checked_add(fee).ok_or(ConfigError::Notional)?;
  if full>plan.order_quote_atoms_e18||full>self.policy.nominal_order_e18
    ||full>self.policy.quote_cap_e18{return Err(ConfigError::Notional)}
  Ok(ReadOnlyRiskPreview{config_sha256:self.sha256.clone(),
   spot_notional_e18:s,futures_notional_e18:f,quote_with_reserved_fees_e18:full,
   may_send_order:false,may_promote_hedged:false})
 }
 /// Actual per-fill quote spent including fees. No invented cumulative fees.
 pub fn preview_actual_spot(&self,plan:&NativeEntryPlan,ledger:&LegFillLedger)
  ->Result<u128,ConfigError>{
  if ledger.venue!="okx"||ledger.side!="buy"
    ||ledger.symbol!=plan.spot_symbol||!ledger.terminal
    ||!ledger.cumulative_truth_present
    ||!ledger.fee_evidence_complete||ledger.fills.is_empty()
    ||ledger.filled_qty_atoms().checked_mul(1_000_000)
     .filter(|q|*q<=plan.planned_qty_atoms_e18).is_none(){
   return Err(ConfigError::Unsupported)
  }
  let mut spent_e12=0u128;
  for fill in ledger.fills.values(){
   let fee=fill.fee_cost_quote_atoms.ok_or(ConfigError::Unsupported)?;
   if fee<0{return Err(ConfigError::Unsupported)}
   let cost=ceil(fill.qty_atoms.checked_mul(fill.price_atoms)
    .ok_or(ConfigError::Notional)?,SCALE_12)?;
   spent_e12=spent_e12.checked_add(cost).and_then(|v|v.checked_add(fee as u128))
    .ok_or(ConfigError::Notional)?;
  }
  let total=spent_e12.checked_mul(1_000_000).ok_or(ConfigError::Notional)?;
  if total>self.policy.nominal_order_e18||total>self.policy.quote_cap_e18
    ||total>plan.order_quote_atoms_e18{return Err(ConfigError::Notional)}
  Ok(total)
 }
}

#[cfg(test)]
mod p106_r146_native_safety_source_tests{
 use super::*;
 const SOURCE:&str=include_str!("../config/native_trading.yaml");
 #[test]
 fn r146_eight_native_and_real_trial_yaml_hard_stops_deny_before_rust_trade(){
  let approved_readonly=NativeTradingConfig::parse(SOURCE).unwrap();
  assert_eq!(approved_readonly.policy.real_daily_trades,0);
  assert_eq!(approved_readonly.policy.real_max_positions,1);
  assert!(!approved_readonly.policy.real_execution_authorized);
  let changes=[
   ("  config_schema_version: 1","  config_schema_version: 2"),
   ("  mutation_capability_enabled: false","  mutation_capability_enabled: true"),
   ("  production_cutover_authorized: false","  production_cutover_authorized: true"),
   ("  real_execution_authorized: false","  real_execution_authorized: true"),
   ("  max_trades_per_day: 0","  max_trades_per_day: 1"),
   ("  max_open_positions: 1\n  preflight_path:",
    "  max_open_positions: 2\n  preflight_path:"),
   ("  deny_on_unknown: true","  deny_on_unknown: false"),
   ("  deny_on_untracked_assets: true","  deny_on_untracked_assets: false"),
  ];
  assert_eq!(changes.len(),8);
  for (old,new) in changes{
   let alternate=SOURCE.replacen(old,new,1);
   assert_ne!(alternate,SOURCE,"original YAML fixture field missing: {old}");
   assert!(NativeTradingConfig::parse(&alternate).is_err(),
     "invalid safety source admitted before REVIEW: {old}");
  }
 }
 #[test]
 fn r146_original_financial_caps_change_known_lower_bound_threshold_only(){
  let src=NativeTradingConfig::parse(SOURCE).unwrap();
  let e18=1_000_000_000_000_000_000u128;
  let known_loss=9*e18+e18/2;
  let known_fee=6*e18+e18/2;
  let original=src.inspect_financial_day_source_limits_without_io(
    Some(known_loss),Some(known_fee)).unwrap();
  assert_eq!(original.source_daily_loss_cap_e18,10*e18);
  assert_eq!(original.source_daily_okx_fee_cap_e18,7*e18);
  assert_eq!(original.claimed_loss_cap_reached,Some(false));
  assert_eq!(original.claimed_fee_cap_reached,Some(false));
  let mutated_yaml=SOURCE
    .replacen("  max_daily_loss_usdt: 10.0","  max_daily_loss_usdt: 9.0",1)
    .replacen("  max_okx_fees_per_day_usdt: 7.0","  max_okx_fees_per_day_usdt: 6.0",1);
  assert_ne!(mutated_yaml,SOURCE);
  let reduced=NativeTradingConfig::parse(&mutated_yaml).unwrap()
    .inspect_financial_day_source_limits_without_io(
      Some(known_loss),Some(known_fee)).unwrap();
  assert_eq!(reduced.source_daily_loss_cap_e18,9*e18);
  assert_eq!(reduced.source_daily_okx_fee_cap_e18,6*e18);
  assert_eq!(reduced.claimed_loss_cap_reached,Some(true));
  assert_eq!(reduced.claimed_fee_cap_reached,Some(true));
  for review in [original,reduced]{
   assert!(!review.entire_exchange_day_authenticated);
   assert!(!review.missing_entries_or_late_fees_excluded);
   assert!(!review.source_applied_to_installed_paper);
   assert!(!review.may_open_real);
   assert!(!review.may_mutate_paper_or_account);
  }
 }
 #[test]
 fn r146_each_original_money_limit_independently_changes_local_threshold(){
  let base=NativeTradingConfig::parse(SOURCE).unwrap();
  let e18=1_000_000_000_000_000_000u128;
  let check=|cfg:&NativeTradingConfig|cfg
    .inspect_financial_day_source_limits_without_io(
      Some(9*e18+e18/2),Some(6*e18+e18/2)).unwrap();
  let control=check(&base);
  for (old,new,loss_expected,fee_expected) in [
   ("  max_daily_loss_usdt: 10.0","  max_daily_loss_usdt: 9.0",
     Some(true),Some(false)),
   ("  max_okx_fees_per_day_usdt: 7.0",
     "  max_okx_fees_per_day_usdt: 6.0",
     Some(false),Some(true)),
  ]{
   let changed=SOURCE.replacen(old,new,1);
   assert_ne!(changed,SOURCE);
   let review=check(&NativeTradingConfig::parse(&changed).unwrap());
   assert_eq!(review.claimed_loss_cap_reached,loss_expected);
   assert_eq!(review.claimed_fee_cap_reached,fee_expected);
   assert_ne!(review,control);
   assert!(!review.may_open_real);
   assert!(!review.entire_exchange_day_authenticated);
  }
 }
 #[test]
 fn r146_missing_source_observations_are_unknown_never_zero_or_authority(){
  let src=NativeTradingConfig::parse(SOURCE).unwrap();
  let unknown=src.inspect_financial_day_source_limits_without_io(None,None).unwrap();
  assert_eq!(unknown.claimed_loss_e18,None);
  assert_eq!(unknown.claimed_okx_fees_e18,None);
  assert_eq!(unknown.claimed_loss_cap_reached,None);
  assert_eq!(unknown.claimed_fee_cap_reached,None);
  assert!(!unknown.may_open_real);
  let known_zero=src.inspect_financial_day_source_limits_without_io(
    Some(0),Some(0)).unwrap();
  assert_eq!(known_zero.claimed_loss_cap_reached,Some(false));
  assert_eq!(known_zero.claimed_fee_cap_reached,Some(false));
  assert!(!known_zero.entire_exchange_day_authenticated);
  assert!(!known_zero.may_open_real);
  let mut altered=src.clone();
  altered.policy.daily_okx_fee_cap_e18=1;
  assert_eq!(altered.inspect_financial_day_source_limits_without_io(None,None),
     Err(ConfigError::Policy));
  let mut altered=src;
  altered.raw["real_trial"]["deny_on_unknown"]=serde_yaml::Value::Bool(false);
  assert_eq!(altered.inspect_financial_day_source_limits_without_io(None,None),
     Err(ConfigError::Policy));
 }
}

#[cfg(test)]
mod r147_real_trial_guard_source_tests{
 use super::*;
 // Historical what-if source behavior: use the immutable ORIGINAL
 // migration baseline, not R1075's deliberately REAL-disabled native YAML.
 // Reconstruct historical TRUE trial flag in TEST MEMORY ONLY.
 // R1075 native source remains REAL=false on disk.
 fn source_fixture()->String{
  let baseline=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  assert_eq!(baseline.matches("real_trial:\n  enabled: false").count(),1);
  baseline.replacen("real_trial:\n  enabled: false",
    "real_trial:\n  enabled: true",1)
 }
 fn claims()->NativeRealTrialClaimedAges{
  NativeRealTrialClaimedAges{
   last_entry_age_sec:Some(10_800),
   preflight_age_sec:Some(2_000),
   mirror_lag_records:Some(20),
   kill_disengaged_age_sec:Some(700),
   kill_drill_age_sec:Some(100_000),
   telegram_delivery_age_sec:Some(2_000),
  }
 }
 fn review(text:&str)->NativeRealTrialGuardWindowReview{
  let cfg=NativeTradingConfig::parse(text).unwrap();
  cfg.inspect_real_trial_guard_windows_readonly(&claims()).unwrap()
 }
 #[test]
 fn r147_eight_original_source_fields_independently_change_local_guard_outcomes(){
  let source=source_fixture();
  let base=review(source.as_str());
  assert!(base.source_trial_enabled);
  assert!(base.source_bootstrap_first_trade);
  assert_eq!(base.source_entry_cooldown_sec,10800);
  assert_eq!(base.source_preflight_max_age_sec,3600);
  assert_eq!(base.source_max_mirror_lag_records,32);
  assert_eq!(base.source_kill_disengaged_max_age_sec,900);
  assert_eq!(base.source_kill_drill_max_age_sec,2592000);
  assert_eq!(base.source_telegram_delivery_max_age_sec,3600);
  for yes in [
   base.local_cooldown_elapsed_claim,
   base.local_preflight_fresh_claim,
   base.local_mirror_lag_acceptable_claim,
   base.local_kill_disengagement_fresh_claim,
   base.local_kill_drill_fresh_claim,
   base.local_telegram_delivery_fresh_claim,
  ]{assert_eq!(yes,Some(true))}
  let table=[
   ("  enabled: true\n  operator_approved:", "  enabled: false\n  operator_approved:"),
   ("  bootstrap_first_trade: true","  bootstrap_first_trade: false"),
   ("  entry_cooldown_sec: 10800","  entry_cooldown_sec: 10801"),
   ("  preflight_max_age_sec: 3600","  preflight_max_age_sec: 1800"),
   ("  max_mirror_lag_records: 32","  max_mirror_lag_records: 16"),
   ("  kill_switch_disengaged_max_age_sec: 900","  kill_switch_disengaged_max_age_sec: 600"),
   ("  kill_switch_drill_max_age_sec: 2592000","  kill_switch_drill_max_age_sec: 86400"),
   ("  telegram_delivery_max_age_sec: 3600","  telegram_delivery_max_age_sec: 1800"),
  ];
  assert_eq!(table.len(),8);
  for (i,(old,new)) in table.into_iter().enumerate(){
   let changed=source.replacen(old,new,1);
   assert_ne!(changed,source.as_str(),"source fixture missing {i}: {old}");
   let newer=review(&changed);
   let altered=match i{
    0=>!newer.source_trial_enabled,
    1=>!newer.source_bootstrap_first_trade,
    2=>newer.local_cooldown_elapsed_claim==Some(false),
    3=>newer.local_preflight_fresh_claim==Some(false),
    4=>newer.local_mirror_lag_acceptable_claim==Some(false),
    5=>newer.local_kill_disengagement_fresh_claim==Some(false),
    6=>newer.local_kill_drill_fresh_claim==Some(false),
    _=>newer.local_telegram_delivery_fresh_claim==Some(false),
   };
   assert!(altered,"no per-field guard effect for case {i}");
   assert!(!newer.may_launch_real&&!newer.may_modify_live_orders_or_paper);
   assert!(!newer.installed_paper_source_attested);
   assert!(!newer.kill_switch_loaded_or_disengaged);
   assert!(!newer.preflight_loaded_or_verified);
  }
 }
 #[test]
 fn r147_unknown_sources_never_become_zero_or_real_grant(){
  let source=source_fixture();
  let config=NativeTradingConfig::parse(source.as_str()).unwrap();
  let r=config.inspect_real_trial_guard_windows_readonly(
    &NativeRealTrialClaimedAges::default()).unwrap();
  for missing in [
   r.local_cooldown_elapsed_claim,r.local_preflight_fresh_claim,
   r.local_mirror_lag_acceptable_claim,
   r.local_kill_disengagement_fresh_claim,
   r.local_kill_drill_fresh_claim,
   r.local_telegram_delivery_fresh_claim,
  ]{assert_eq!(missing,None)}
  assert!(!r.has_exchange_authenticated_facts);
  assert!(!r.preflight_loaded_or_verified);
  assert!(!r.kill_switch_loaded_or_disengaged);
  assert!(!r.mirror_owner_state_authenticated);
  assert!(!r.cooldown_state_durably_verified);
  assert!(!r.telegram_delivery_authenticated);
  assert!(!r.may_launch_real);
  let claimed=review(source.as_str());
  assert_eq!(claimed.local_preflight_fresh_claim,Some(true));
  assert!(!claimed.may_launch_real,
   "all locally plausible claims cannot grant real authority");
 }
 #[test]
 fn r147_invalid_windows_and_postparse_changes_fail_closed(){
  let source=source_fixture();
  for (old,new) in [
   ("  preflight_max_age_sec: 3600","  preflight_max_age_sec: 0"),
   ("  max_mirror_lag_records: 32","  max_mirror_lag_records: 0"),
   ("  kill_switch_disengaged_max_age_sec: 900",
      "  kill_switch_disengaged_max_age_sec: 0"),
   ("  kill_switch_drill_max_age_sec: 2592000",
      "  kill_switch_drill_max_age_sec: 2592001"),
   ("  telegram_delivery_max_age_sec: 3600",
      "  telegram_delivery_max_age_sec: 0"),
  ]{
   let changed=source.replacen(old,new,1);
   assert_ne!(changed,source.as_str());
   let cfg=NativeTradingConfig::parse(&changed).unwrap();
   assert_eq!(cfg.inspect_real_trial_guard_windows_readonly(&claims()),
      Err(ConfigError::Policy));
  }
  let mut cfg=NativeTradingConfig::parse(source.as_str()).unwrap();
  cfg.raw["real_trial"]["bootstrap_first_trade"]=Value::Bool(false);
  assert_eq!(cfg.inspect_real_trial_guard_windows_readonly(&claims()),
    Err(ConfigError::Policy));
  let mut cfg=NativeTradingConfig::parse(source.as_str()).unwrap();
  cfg.policy.real_execution_authorized=true;
  assert_eq!(cfg.inspect_real_trial_guard_windows_readonly(&claims()),
    Err(ConfigError::Policy));
 }
}

#[cfg(test)]
mod r149_legacy_entry_budget_source_tests{
 use super::*;
 // These tests compare historical trade_enabled:true/false behavior.
 // R1075's operator-safe native YAML is FALSE by design; source
 // lineage tests must remain anchored to the original baseline.
 // Historical trade_enabled=true/false guard test, never a runtime config.
 fn source_fixture()->String{
  let baseline=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  assert_eq!(baseline.matches("  trade_enabled: false").count(),1);
  baseline.replacen("  trade_enabled: false","  trade_enabled: true",1)
 }
 #[test]
 fn r149_two_distinct_yaml_fields_change_legacy_local_reject_without_trade(){
  let s=source_fixture();
  let baseline=NativeTradingConfig::parse(s.as_str()).unwrap();
  let initial=baseline.inspect_legacy_general_entry_budget_readonly(Some(150)).unwrap();
  assert_eq!(initial.source_legacy_trade_enabled,true);
  assert_eq!(initial.source_general_daily_trade_cap,300);
  assert_eq!(initial.locally_reject_candidate,Some(false));
  let off=s.replacen("  trade_enabled: true","  trade_enabled: false",1);
  assert_ne!(off,s.as_str());
  let disabled=NativeTradingConfig::parse(&off).unwrap()
    .inspect_legacy_general_entry_budget_readonly(Some(150)).unwrap();
  assert_eq!(disabled.source_legacy_trade_enabled,false);
  assert_eq!(disabled.locally_reject_candidate,Some(true));
  let narrow=s.replacen("  max_trades_per_day: 300",
     "  max_trades_per_day: 100",1);
  assert_ne!(narrow,s.as_str());
  let lowered=NativeTradingConfig::parse(&narrow).unwrap()
    .inspect_legacy_general_entry_budget_readonly(Some(150)).unwrap();
  assert_eq!(lowered.source_general_daily_trade_cap,100);
  assert_eq!(lowered.locally_reject_candidate,Some(true));
  assert_eq!(lowered.source_legacy_trade_enabled,true);
  for review in [initial,disabled,lowered]{
   assert!(!review.account_day_complete_or_authenticated);
   assert!(!review.paper_quota_binding_verified);
   assert!(!review.real_launch_authorized);
   assert!(!review.may_send_or_cancel_order);
   assert!(!review.installed_paper_source_attested);
  }
 }
 #[test]
 fn r149_unknown_day_count_and_source_drift_fail_closed(){
  let s=source_fixture();
  let cfg=NativeTradingConfig::parse(s.as_str()).unwrap();
  let unknown=cfg.inspect_legacy_general_entry_budget_readonly(None).unwrap();
  assert_eq!(unknown.caller_known_daily_trades,None);
  assert_eq!(unknown.locally_reject_candidate,None);
  assert_eq!(cfg.inspect_legacy_general_entry_budget_readonly(Some(300))
     .unwrap().locally_reject_candidate,Some(true));
  assert_eq!(cfg.inspect_legacy_general_entry_budget_readonly(Some(299))
     .unwrap().locally_reject_candidate,Some(false));
  let off=s.replacen("  trade_enabled: true","  trade_enabled: false",1);
  assert_eq!(NativeTradingConfig::parse(&off).unwrap()
    .inspect_legacy_general_entry_budget_readonly(None)
    .unwrap().locally_reject_candidate,Some(true));
  let mut cfg=cfg;
  cfg.raw["general"]["trade_enabled"]=Value::Bool(false);
  assert_eq!(cfg.inspect_legacy_general_entry_budget_readonly(None),
    Err(ConfigError::Policy));
 }
}
