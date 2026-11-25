//! R12 execution-free hard safety gate. Missing or invalid facts deny.
use serde::{Deserialize,Serialize};

#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct SafetyInput {
 pub trade_enabled:bool,pub trial_enabled:bool,pub operator_approved:bool,
 pub max_trades_per_day:i64,pub entry_cooldown_sec:i64,pub max_open_positions:i64,
 pub deny_on_unknown:bool,pub deny_on_untracked_assets:bool,
 pub position_state_valid:bool,pub active_position:bool,pub paper_position:bool,
 pub unknown_or_manual_state:bool,
 pub preflight_present:bool,pub preflight_path_ok:bool,pub preflight_read_ok:bool,
 pub preflight_size_ok:bool,pub preflight_valid:bool,pub preflight_max_age_ok:bool,
 pub preflight_schema_ok:bool,pub preflight_fresh:bool,pub preflight_allow:bool,
 pub preflight_has_blockers:bool,pub exchange_polled:bool,pub live_checks_complete:bool,
 pub live_reconciliation_complete:bool,pub outbox_unresolved:u64,
 pub spot_inventory_policy_valid:bool,pub inventory_policy_match:bool,
 pub untracked_spot:bool,pub untracked_futures:bool,pub untracked_orders:bool,
 pub untracked_exchange_assets:bool,pub lifecycle_verified:bool,
 pub bootstrap_policy_valid:bool,pub bootstrap_valid:bool,pub prior_incident_valid:bool,
 pub cooldown_evidence_present:bool,pub cooldown_state_present:bool,
 pub cooldown_evidence_valid:bool,pub cooldown_remaining_sec:u64,
 pub auto_preflight_refresh_failed:bool,
 pub kill_path_ok:bool,pub kill_read_ok:bool,pub kill_size_ok:bool,pub kill_payload_ok:bool,
 pub kill_switch_present:bool,pub kill_switch_schema_ok:bool,pub kill_switch_operator_ok:bool,
 pub kill_switch_state:String,pub kill_drill_age_ok:bool,pub kill_drilled:bool,
 pub kill_switch_drill_fresh:bool,pub kill_max_age_ok:bool,pub kill_switch_disengagement_fresh:bool,
}
#[derive(Clone,Debug,Serialize,PartialEq,Eq)]
pub struct SafetyDecision {
 pub allowed:bool,pub verdict:String,pub blocker_codes:Vec<String>,
 pub preflight_is_not_launch_authority:bool,pub real_launch_authorized:bool,
}
pub const PYTHON_SAFETY_BLOCKERS:[&str;55]=[
"BLOCKER_ACTIVE_POSITION_PRESENT","BLOCKER_AUTO_PREFLIGHT_REFRESH_FAILED","BLOCKER_FIRST_TRADE_BOOTSTRAP_INVALID","BLOCKER_FIRST_TRADE_BOOTSTRAP_POLICY_INVALID","BLOCKER_INVENTORY_POLICY_MISMATCH",
"BLOCKER_KILL_SWITCH_DISENGAGEMENT_STALE","BLOCKER_KILL_SWITCH_DRILL_MAX_AGE_INVALID","BLOCKER_KILL_SWITCH_DRILL_STALE","BLOCKER_KILL_SWITCH_ENGAGED","BLOCKER_KILL_SWITCH_INVALID",
"BLOCKER_KILL_SWITCH_MAX_AGE_INVALID","BLOCKER_KILL_SWITCH_MISSING","BLOCKER_KILL_SWITCH_NOT_DRILLED","BLOCKER_KILL_SWITCH_OPERATOR_INVALID","BLOCKER_KILL_SWITCH_PATH_INVALID",
"BLOCKER_KILL_SWITCH_READ_FAILED","BLOCKER_KILL_SWITCH_SCHEMA_INVALID","BLOCKER_KILL_SWITCH_STATE_INVALID","BLOCKER_KILL_SWITCH_TOO_LARGE","BLOCKER_LEG_LIFECYCLE_NOT_VERIFIED",
"BLOCKER_LIVE_CHECKS_INCOMPLETE","BLOCKER_LIVE_EXCHANGE_NOT_POLLED","BLOCKER_LIVE_RECONCILIATION_INCOMPLETE","BLOCKER_OPERATOR_APPROVAL_MISSING","BLOCKER_OUTBOX_UNRESOLVED",
"BLOCKER_PAPER_POSITIONS_ACTIVE","BLOCKER_POSITION_STATE_INVALID","BLOCKER_PREFLIGHT_DENY","BLOCKER_PREFLIGHT_HAS_BLOCKERS","BLOCKER_PREFLIGHT_INVALID",
"BLOCKER_PREFLIGHT_MAX_AGE_INVALID","BLOCKER_PREFLIGHT_MISSING","BLOCKER_PREFLIGHT_PATH_INVALID","BLOCKER_PREFLIGHT_READ_FAILED","BLOCKER_PREFLIGHT_SCHEMA_MISMATCH",
"BLOCKER_PREFLIGHT_STALE","BLOCKER_PREFLIGHT_TOO_LARGE","BLOCKER_PRIOR_INCIDENT_INVALID","BLOCKER_REAL_DAILY_LIMIT_NOT_ZERO","BLOCKER_REAL_ENTRY_COOLDOWN",
"BLOCKER_REAL_ENTRY_COOLDOWN_EVIDENCE_MISSING","BLOCKER_REAL_ENTRY_COOLDOWN_INVALID","BLOCKER_REAL_ENTRY_COOLDOWN_STATE_INVALID","BLOCKER_REAL_ENTRY_COOLDOWN_STATE_MISSING","BLOCKER_REAL_OPEN_LIMIT_NOT_ONE",
"BLOCKER_REAL_TRIAL_DISABLED","BLOCKER_SPOT_INVENTORY_POLICY_INVALID","BLOCKER_TRADE_ENABLED_FALSE","BLOCKER_UNCLASSIFIED_SPOT_ASSETS","BLOCKER_UNKNOWN_OR_MANUAL_STATE",
"BLOCKER_UNKNOWN_POLICY_DISABLED","BLOCKER_UNTRACKED_EXCHANGE_ASSETS","BLOCKER_UNTRACKED_FUTURES_EXPOSURE","BLOCKER_UNTRACKED_OPEN_ORDERS","BLOCKER_UNTRACKED_POLICY_DISABLED"];
fn add(v:&mut Vec<String>,s:&str){if !v.iter().any(|x|x==s){v.push(s.into())}}
pub fn evaluate(i:&SafetyInput)->SafetyDecision{
 let mut b=Vec::new();
 if !i.trade_enabled{add(&mut b,"BLOCKER_TRADE_ENABLED_FALSE")}
 if !i.trial_enabled{add(&mut b,"BLOCKER_REAL_TRIAL_DISABLED")}
 if !i.operator_approved{add(&mut b,"BLOCKER_OPERATOR_APPROVAL_MISSING")}
 if i.max_trades_per_day!=0{add(&mut b,"BLOCKER_REAL_DAILY_LIMIT_NOT_ZERO")}
 if i.entry_cooldown_sec!=10800{add(&mut b,"BLOCKER_REAL_ENTRY_COOLDOWN_INVALID")}
 if i.max_open_positions!=1{add(&mut b,"BLOCKER_REAL_OPEN_LIMIT_NOT_ONE")}
 if !i.deny_on_unknown{add(&mut b,"BLOCKER_UNKNOWN_POLICY_DISABLED")}
 if !i.deny_on_untracked_assets{add(&mut b,"BLOCKER_UNTRACKED_POLICY_DISABLED")}
 if !i.position_state_valid{add(&mut b,"BLOCKER_POSITION_STATE_INVALID")}
 if i.paper_position{add(&mut b,"BLOCKER_PAPER_POSITIONS_ACTIVE")}
 if i.active_position{add(&mut b,"BLOCKER_ACTIVE_POSITION_PRESENT")}
 if i.unknown_or_manual_state{add(&mut b,"BLOCKER_UNKNOWN_OR_MANUAL_STATE")}
 if !i.preflight_path_ok{add(&mut b,"BLOCKER_PREFLIGHT_PATH_INVALID")}
 if !i.preflight_read_ok{add(&mut b,"BLOCKER_PREFLIGHT_READ_FAILED")}
 if !i.preflight_size_ok{add(&mut b,"BLOCKER_PREFLIGHT_TOO_LARGE")}
 if !i.preflight_valid{add(&mut b,"BLOCKER_PREFLIGHT_INVALID")}
 if !i.preflight_present{add(&mut b,"BLOCKER_PREFLIGHT_MISSING")}
 if !i.preflight_max_age_ok{add(&mut b,"BLOCKER_PREFLIGHT_MAX_AGE_INVALID")}
 if !i.preflight_schema_ok{add(&mut b,"BLOCKER_PREFLIGHT_SCHEMA_MISMATCH")}
 if !i.preflight_fresh{add(&mut b,"BLOCKER_PREFLIGHT_STALE")}
 if !i.preflight_allow{add(&mut b,"BLOCKER_PREFLIGHT_DENY")}
 if i.preflight_has_blockers{add(&mut b,"BLOCKER_PREFLIGHT_HAS_BLOCKERS")}
 if !i.exchange_polled{add(&mut b,"BLOCKER_LIVE_EXCHANGE_NOT_POLLED")}
 if !i.live_checks_complete{add(&mut b,"BLOCKER_LIVE_CHECKS_INCOMPLETE")}
 if !i.live_reconciliation_complete{add(&mut b,"BLOCKER_LIVE_RECONCILIATION_INCOMPLETE")}
 if i.outbox_unresolved!=0{add(&mut b,"BLOCKER_OUTBOX_UNRESOLVED")}
 if !i.spot_inventory_policy_valid{add(&mut b,"BLOCKER_SPOT_INVENTORY_POLICY_INVALID")}
 if !i.inventory_policy_match{add(&mut b,"BLOCKER_INVENTORY_POLICY_MISMATCH")}
 if i.untracked_spot{add(&mut b,"BLOCKER_UNCLASSIFIED_SPOT_ASSETS")}
 if i.untracked_futures{add(&mut b,"BLOCKER_UNTRACKED_FUTURES_EXPOSURE")}
 if i.untracked_orders{add(&mut b,"BLOCKER_UNTRACKED_OPEN_ORDERS")}
 if i.untracked_exchange_assets{add(&mut b,"BLOCKER_UNTRACKED_EXCHANGE_ASSETS")}
 if !i.bootstrap_policy_valid{add(&mut b,"BLOCKER_FIRST_TRADE_BOOTSTRAP_POLICY_INVALID")}
 if !i.bootstrap_valid{add(&mut b,"BLOCKER_FIRST_TRADE_BOOTSTRAP_INVALID")}
 if !i.prior_incident_valid{add(&mut b,"BLOCKER_PRIOR_INCIDENT_INVALID")}
 if !i.lifecycle_verified{add(&mut b,"BLOCKER_LEG_LIFECYCLE_NOT_VERIFIED")}
 if !i.cooldown_evidence_present{add(&mut b,"BLOCKER_REAL_ENTRY_COOLDOWN_EVIDENCE_MISSING")}
 if !i.cooldown_state_present{add(&mut b,"BLOCKER_REAL_ENTRY_COOLDOWN_STATE_MISSING")}
 if !i.cooldown_evidence_valid{add(&mut b,"BLOCKER_REAL_ENTRY_COOLDOWN_STATE_INVALID")}
 else if i.cooldown_remaining_sec>0{add(&mut b,"BLOCKER_REAL_ENTRY_COOLDOWN")}
 if i.auto_preflight_refresh_failed{add(&mut b,"BLOCKER_AUTO_PREFLIGHT_REFRESH_FAILED")}
 if !i.kill_path_ok{add(&mut b,"BLOCKER_KILL_SWITCH_PATH_INVALID")}
 if !i.kill_read_ok{add(&mut b,"BLOCKER_KILL_SWITCH_READ_FAILED")}
 if !i.kill_size_ok{add(&mut b,"BLOCKER_KILL_SWITCH_TOO_LARGE")}
 if !i.kill_payload_ok{add(&mut b,"BLOCKER_KILL_SWITCH_INVALID")}
 if !i.kill_switch_present{add(&mut b,"BLOCKER_KILL_SWITCH_MISSING")}
 else {
  if !i.kill_switch_schema_ok{add(&mut b,"BLOCKER_KILL_SWITCH_SCHEMA_INVALID")}
  if !i.kill_switch_operator_ok{add(&mut b,"BLOCKER_KILL_SWITCH_OPERATOR_INVALID")}
  if i.kill_switch_state!="DISENGAGED"{
   add(&mut b,if i.kill_switch_state=="ENGAGED"{"BLOCKER_KILL_SWITCH_ENGAGED"}else{"BLOCKER_KILL_SWITCH_STATE_INVALID"})
  }
  if !i.kill_drill_age_ok{add(&mut b,"BLOCKER_KILL_SWITCH_DRILL_MAX_AGE_INVALID")}
  if !i.kill_drilled{add(&mut b,"BLOCKER_KILL_SWITCH_NOT_DRILLED")}
  if !i.kill_switch_drill_fresh{add(&mut b,"BLOCKER_KILL_SWITCH_DRILL_STALE")}
  if !i.kill_max_age_ok{add(&mut b,"BLOCKER_KILL_SWITCH_MAX_AGE_INVALID")}
  if !i.kill_switch_disengagement_fresh{add(&mut b,"BLOCKER_KILL_SWITCH_DISENGAGEMENT_STALE")}
 }
 let allowed=b.is_empty();
 SafetyDecision{
  allowed,verdict:if allowed{"ALLOW".into()}else{"DENY".into()},blocker_codes:b,
  preflight_is_not_launch_authority:true,real_launch_authorized:false,
 }
}

#[cfg(test)]
mod tests{
 use super::*;
 fn good()->SafetyInput{SafetyInput{
  trade_enabled:true,trial_enabled:true,operator_approved:true,max_trades_per_day:0,
  entry_cooldown_sec:10800,max_open_positions:1,deny_on_unknown:true,deny_on_untracked_assets:true,
  position_state_valid:true,active_position:false,paper_position:false,unknown_or_manual_state:false,
  preflight_present:true,preflight_path_ok:true,preflight_read_ok:true,preflight_size_ok:true,preflight_valid:true,preflight_max_age_ok:true,preflight_schema_ok:true,preflight_fresh:true,preflight_allow:true,preflight_has_blockers:false,
  exchange_polled:true,live_checks_complete:true,live_reconciliation_complete:true,outbox_unresolved:0,
  spot_inventory_policy_valid:true,inventory_policy_match:true,untracked_spot:false,untracked_futures:false,untracked_orders:false,
  untracked_exchange_assets:false,lifecycle_verified:true,bootstrap_policy_valid:true,bootstrap_valid:true,prior_incident_valid:true,cooldown_evidence_present:true,cooldown_state_present:true,cooldown_evidence_valid:true,
  cooldown_remaining_sec:0,auto_preflight_refresh_failed:false,kill_path_ok:true,kill_read_ok:true,kill_size_ok:true,kill_payload_ok:true,kill_switch_present:true,
  kill_switch_schema_ok:true,kill_switch_operator_ok:true,kill_switch_state:"DISENGAGED".into(),
  kill_drill_age_ok:true,kill_drilled:true,kill_switch_drill_fresh:true,kill_max_age_ok:true,
  kill_switch_disengagement_fresh:true}}
 #[test]fn all_facts_allow_but_never_launch_authority(){
  let r=evaluate(&good());assert!(r.allowed);assert_eq!(r.verdict,"ALLOW");
  assert!(r.preflight_is_not_launch_authority);assert!(!r.real_launch_authorized);
 }
 #[test]fn kill_switch_engaged_denies(){
  let mut i=good();i.kill_switch_state="ENGAGED".into();
  assert!(evaluate(&i).blocker_codes.contains(&"BLOCKER_KILL_SWITCH_ENGAGED".into()));
 }
 #[test]fn missing_kill_switch_denies(){let mut i=good();i.kill_switch_present=false;assert!(!evaluate(&i).allowed);}
 #[test]fn active_position_denies_after_cooldown(){let mut i=good();i.active_position=true;assert!(!evaluate(&i).allowed);}
 #[test]fn outbox_and_untracked_deny(){
  let mut i=good();i.outbox_unresolved=1;i.untracked_futures=true;
  let r=evaluate(&i);assert_eq!(r.verdict,"DENY");assert_eq!(r.blocker_codes.len(),2);
 }
 #[test]fn stale_preflight_denies(){let mut i=good();i.preflight_fresh=false;assert!(!evaluate(&i).allowed);}
 #[test]fn cooldown_denies(){let mut i=good();i.cooldown_remaining_sec=1;assert!(!evaluate(&i).allowed);}
 #[test]fn malformed_facts_deny_and_never_authorize(){
  let mut i=good();i.trade_enabled=false;i.operator_approved=false;i.position_state_valid=false;
  i.preflight_present=false;i.kill_switch_present=false;let r=evaluate(&i);
  assert!(!r.allowed);assert!(r.blocker_codes.len()>=5);assert!(!r.real_launch_authorized);
 }
}

#[cfg(test)]
mod matrix_tests{
 use super::*;
 fn good()->SafetyInput{SafetyInput{
  trade_enabled:true,trial_enabled:true,operator_approved:true,max_trades_per_day:0,
  entry_cooldown_sec:10800,max_open_positions:1,deny_on_unknown:true,deny_on_untracked_assets:true,
  position_state_valid:true,active_position:false,paper_position:false,unknown_or_manual_state:false,
  preflight_present:true,preflight_path_ok:true,preflight_read_ok:true,preflight_size_ok:true,preflight_valid:true,preflight_max_age_ok:true,preflight_schema_ok:true,preflight_fresh:true,preflight_allow:true,preflight_has_blockers:false,
  exchange_polled:true,live_checks_complete:true,live_reconciliation_complete:true,outbox_unresolved:0,
  spot_inventory_policy_valid:true,inventory_policy_match:true,untracked_spot:false,untracked_futures:false,untracked_orders:false,
  untracked_exchange_assets:false,lifecycle_verified:true,bootstrap_policy_valid:true,bootstrap_valid:true,prior_incident_valid:true,cooldown_evidence_present:true,cooldown_state_present:true,cooldown_evidence_valid:true,cooldown_remaining_sec:0,
  auto_preflight_refresh_failed:false,kill_path_ok:true,kill_read_ok:true,kill_size_ok:true,kill_payload_ok:true,kill_switch_present:true,kill_switch_schema_ok:true,
  kill_switch_operator_ok:true,kill_switch_state:"DISENGAGED".into(),kill_drill_age_ok:true,kill_drilled:true,kill_switch_drill_fresh:true,kill_max_age_ok:true,
  kill_switch_disengagement_fresh:true}}
 fn deny(mut i:SafetyInput,code:&str){let r=evaluate(&i);assert!(!r.allowed,"{code}");assert!(r.blocker_codes.iter().any(|x|x==code),"{code}: {:?}",r.blocker_codes);assert!(!r.real_launch_authorized);i.trade_enabled=i.trade_enabled;}
 #[test]fn full_single_fault_deny_matrix(){
  let cases:[(&str,fn(&mut SafetyInput));24]=[
   ("BLOCKER_TRADE_ENABLED_FALSE",|x|x.trade_enabled=false),
   ("BLOCKER_REAL_TRIAL_DISABLED",|x|x.trial_enabled=false),
   ("BLOCKER_OPERATOR_APPROVAL_MISSING",|x|x.operator_approved=false),
   ("BLOCKER_REAL_DAILY_LIMIT_NOT_ZERO",|x|x.max_trades_per_day=1),
   ("BLOCKER_REAL_ENTRY_COOLDOWN_INVALID",|x|x.entry_cooldown_sec=1),
   ("BLOCKER_REAL_OPEN_LIMIT_NOT_ONE",|x|x.max_open_positions=2),
   ("BLOCKER_UNKNOWN_POLICY_DISABLED",|x|x.deny_on_unknown=false),
   ("BLOCKER_UNTRACKED_POLICY_DISABLED",|x|x.deny_on_untracked_assets=false),
   ("BLOCKER_POSITION_STATE_INVALID",|x|x.position_state_valid=false),
   ("BLOCKER_PAPER_POSITIONS_ACTIVE",|x|x.paper_position=true),
   ("BLOCKER_ACTIVE_POSITION_PRESENT",|x|x.active_position=true),
   ("BLOCKER_UNKNOWN_OR_MANUAL_STATE",|x|x.unknown_or_manual_state=true),
   ("BLOCKER_PREFLIGHT_MISSING",|x|x.preflight_present=false),
   ("BLOCKER_PREFLIGHT_SCHEMA_MISMATCH",|x|x.preflight_schema_ok=false),
   ("BLOCKER_PREFLIGHT_STALE",|x|x.preflight_fresh=false),
   ("BLOCKER_PREFLIGHT_DENY",|x|x.preflight_allow=false),
   ("BLOCKER_LIVE_EXCHANGE_NOT_POLLED",|x|x.exchange_polled=false),
   ("BLOCKER_LIVE_CHECKS_INCOMPLETE",|x|x.live_checks_complete=false),
   ("BLOCKER_LIVE_RECONCILIATION_INCOMPLETE",|x|x.live_reconciliation_complete=false),
   ("BLOCKER_OUTBOX_UNRESOLVED",|x|x.outbox_unresolved=1),
   ("BLOCKER_INVENTORY_POLICY_MISMATCH",|x|x.inventory_policy_match=false),
   ("BLOCKER_UNCLASSIFIED_SPOT_ASSETS",|x|x.untracked_spot=true),
   ("BLOCKER_UNTRACKED_FUTURES_EXPOSURE",|x|x.untracked_futures=true),
   ("BLOCKER_UNTRACKED_OPEN_ORDERS",|x|x.untracked_orders=true),
  ];
  for (code,mutate) in cases{let mut i=good();mutate(&mut i);deny(i,code)}
 }
 #[test]fn lifecycle_cooldown_and_kill_switch_matrix(){
  let cases:[(&str,fn(&mut SafetyInput));8]=[
   ("BLOCKER_UNTRACKED_EXCHANGE_ASSETS",|x|x.untracked_exchange_assets=true),
   ("BLOCKER_LEG_LIFECYCLE_NOT_VERIFIED",|x|x.lifecycle_verified=false),
   ("BLOCKER_REAL_ENTRY_COOLDOWN_STATE_INVALID",|x|x.cooldown_evidence_valid=false),
   ("BLOCKER_AUTO_PREFLIGHT_REFRESH_FAILED",|x|x.auto_preflight_refresh_failed=true),
   ("BLOCKER_KILL_SWITCH_SCHEMA_INVALID",|x|x.kill_switch_schema_ok=false),
   ("BLOCKER_KILL_SWITCH_OPERATOR_INVALID",|x|x.kill_switch_operator_ok=false),
   ("BLOCKER_KILL_SWITCH_DRILL_STALE",|x|x.kill_switch_drill_fresh=false),
   ("BLOCKER_KILL_SWITCH_DISENGAGEMENT_STALE",|x|x.kill_switch_disengagement_fresh=false),
  ];
  for (code,mutate) in cases{let mut i=good();mutate(&mut i);deny(i,code)}
 }
 #[test]fn invalid_kill_switch_state_denies(){let mut i=good();i.kill_switch_state="BROKEN".into();deny(i,"BLOCKER_KILL_SWITCH_STATE_INVALID");}
 #[test]fn kill_switch_loader_and_age_matrix(){
  let cases:[(&str,fn(&mut SafetyInput));8]=[
   ("BLOCKER_KILL_SWITCH_PATH_INVALID",|x|x.kill_path_ok=false),
   ("BLOCKER_KILL_SWITCH_READ_FAILED",|x|x.kill_read_ok=false),
   ("BLOCKER_KILL_SWITCH_TOO_LARGE",|x|x.kill_size_ok=false),
   ("BLOCKER_KILL_SWITCH_INVALID",|x|x.kill_payload_ok=false),
   ("BLOCKER_KILL_SWITCH_DRILL_MAX_AGE_INVALID",|x|x.kill_drill_age_ok=false),
   ("BLOCKER_KILL_SWITCH_NOT_DRILLED",|x|x.kill_drilled=false),
   ("BLOCKER_KILL_SWITCH_MAX_AGE_INVALID",|x|x.kill_max_age_ok=false),
   ("BLOCKER_KILL_SWITCH_DISENGAGEMENT_STALE",|x|x.kill_switch_disengagement_fresh=false),
  ];
  for (code,mutate) in cases{let mut i=good();mutate(&mut i);deny(i,code)}
 }
 #[test]fn canonical_behavior_vectors(){
  let mut vectors:Vec<(&str,SafetyInput,bool,Vec<&str>)>=Vec::new();
  vectors.push(("all_green",good(),true,vec![]));
  let mut x=good();x.trial_enabled=false;x.operator_approved=false;vectors.push(("trial_operator",x,false,vec!["BLOCKER_REAL_TRIAL_DISABLED","BLOCKER_OPERATOR_APPROVAL_MISSING"]));
  let mut x=good();x.max_trades_per_day=2;x.entry_cooldown_sec=3600;x.max_open_positions=10;vectors.push(("limits",x,false,vec!["BLOCKER_REAL_DAILY_LIMIT_NOT_ZERO","BLOCKER_REAL_ENTRY_COOLDOWN_INVALID","BLOCKER_REAL_OPEN_LIMIT_NOT_ONE"]));
  let mut x=good();x.cooldown_remaining_sec=3600;vectors.push(("cooldown",x,false,vec!["BLOCKER_REAL_ENTRY_COOLDOWN"]));
  let mut x=good();x.preflight_fresh=false;vectors.push(("stale_preflight",x,false,vec!["BLOCKER_PREFLIGHT_STALE"]));
  let mut x=good();x.preflight_allow=false;x.preflight_has_blockers=true;vectors.push(("denied_preflight",x,false,vec!["BLOCKER_PREFLIGHT_DENY","BLOCKER_PREFLIGHT_HAS_BLOCKERS"]));
  let mut x=good();x.paper_position=true;x.unknown_or_manual_state=true;vectors.push(("paper_unknown",x,false,vec!["BLOCKER_PAPER_POSITIONS_ACTIVE","BLOCKER_UNKNOWN_OR_MANUAL_STATE"]));
  let mut x=good();x.active_position=true;vectors.push(("active",x,false,vec!["BLOCKER_ACTIVE_POSITION_PRESENT"]));
  let mut x=good();x.untracked_exchange_assets=true;x.outbox_unresolved=1;vectors.push(("assets_outbox",x,false,vec!["BLOCKER_OUTBOX_UNRESOLVED","BLOCKER_UNTRACKED_EXCHANGE_ASSETS"]));
  let mut x=good();x.kill_switch_state="ENGAGED".into();vectors.push(("kill_engaged",x,false,vec!["BLOCKER_KILL_SWITCH_ENGAGED"]));
  let mut x=good();x.inventory_policy_match=false;vectors.push(("inventory_policy",x,false,vec!["BLOCKER_INVENTORY_POLICY_MISMATCH"]));
  let mut x=good();x.untracked_futures=true;vectors.push(("futures",x,false,vec!["BLOCKER_UNTRACKED_FUTURES_EXPOSURE"]));
  let mut x=good();x.bootstrap_valid=false;vectors.push(("bootstrap",x,false,vec!["BLOCKER_FIRST_TRADE_BOOTSTRAP_INVALID"]));
  let mut x=good();x.preflight_schema_ok=false;vectors.push(("schema",x,false,vec!["BLOCKER_PREFLIGHT_SCHEMA_MISMATCH"]));
  for (name,input,allow,codes) in vectors{let r=evaluate(&input);assert_eq!(r.allowed,allow,"{name}");assert!(!r.real_launch_authorized,"{name}");let expected:Vec<String>=codes.into_iter().map(String::from).collect();assert_eq!(r.blocker_codes,expected,"{name}");}
 }
 #[test]fn python_blocker_catalog_is_complete_and_unique(){
  let mut v=PYTHON_SAFETY_BLOCKERS.to_vec();v.sort_unstable();v.dedup();assert_eq!(v.len(),55);
 }
 #[test]fn combined_failure_and_race_fail_closed(){
  let mut i=good();i.preflight_fresh=false;i.outbox_unresolved=2;i.kill_switch_state="ENGAGED".into();
  let r=evaluate(&i);assert!(!r.allowed);assert!(!r.real_launch_authorized);
  for c in ["BLOCKER_PREFLIGHT_STALE","BLOCKER_OUTBOX_UNRESOLVED","BLOCKER_KILL_SWITCH_ENGAGED"]{assert!(r.blocker_codes.iter().any(|x|x==c));}
  let before=evaluate(&good());assert!(before.allowed);assert!(!before.real_launch_authorized);
  let mut changed=good();changed.kill_switch_state="ENGAGED".into();let after=evaluate(&changed);
  assert!(!after.allowed);assert!(!after.real_launch_authorized);
 }
 #[test]fn extended_python_blocker_matrix(){
  let cases:[(&str,fn(&mut SafetyInput));13]=[
   ("BLOCKER_PREFLIGHT_PATH_INVALID",|x|x.preflight_path_ok=false),
   ("BLOCKER_PREFLIGHT_READ_FAILED",|x|x.preflight_read_ok=false),
   ("BLOCKER_PREFLIGHT_TOO_LARGE",|x|x.preflight_size_ok=false),
   ("BLOCKER_PREFLIGHT_INVALID",|x|x.preflight_valid=false),
   ("BLOCKER_PREFLIGHT_MAX_AGE_INVALID",|x|x.preflight_max_age_ok=false),
   ("BLOCKER_PREFLIGHT_HAS_BLOCKERS",|x|x.preflight_has_blockers=true),
   ("BLOCKER_SPOT_INVENTORY_POLICY_INVALID",|x|x.spot_inventory_policy_valid=false),
   ("BLOCKER_FIRST_TRADE_BOOTSTRAP_POLICY_INVALID",|x|x.bootstrap_policy_valid=false),
   ("BLOCKER_FIRST_TRADE_BOOTSTRAP_INVALID",|x|x.bootstrap_valid=false),
   ("BLOCKER_PRIOR_INCIDENT_INVALID",|x|x.prior_incident_valid=false),
   ("BLOCKER_REAL_ENTRY_COOLDOWN_EVIDENCE_MISSING",|x|x.cooldown_evidence_present=false),
   ("BLOCKER_REAL_ENTRY_COOLDOWN_STATE_MISSING",|x|x.cooldown_state_present=false),
   ("BLOCKER_REAL_ENTRY_COOLDOWN_STATE_INVALID",|x|x.cooldown_evidence_valid=false),
  ];
  for (code,mutate) in cases{let mut i=good();mutate(&mut i);deny(i,code)}
 }
}
