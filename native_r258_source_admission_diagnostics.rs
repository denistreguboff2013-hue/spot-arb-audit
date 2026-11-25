//! R258: deterministic, fail-closed admission diagnostics for two DISTINCT
//! PAPER research lanes. No network, filesystem reads/writes, orders or service.
 //! Results are source-only; they do not attest the installed PAPER.
use crate::native_trading_config::NativeTradingConfig;
use crate::native_paper_config_bridge::{
    admit_public_paper_yaml_review,verify_paper_portfolio_quota_parity,
    verify_public_paper_unwired_runtime_features,
};
use crate::native_paper_strategy_frame_review::SupervisedStrategyPaper;
use crate::native_r174_unwired_legacy_guard::verify_unwired_source_r174;
use crate::native_paper_portfolio_review::{MAX_DAILY_PAPER,MAX_CONCURRENT_PAPER};
use serde_json::{Value,json};
use std::collections::BTreeSet;

fn toggle(c:&NativeTradingConfig,group:&str,field:&str)->Option<bool>{
    c.raw.get(group)?.get(field)?.as_bool()
}
/// Construct a NEW in-memory research owner to check the actual R155 guard,
/// never a production/supervisor owner. This requires no exchange API.
pub fn review_two_paper_admissions_r258(c:&NativeTradingConfig,at_wall_ms:i64)->Value{
    let mut source=BTreeSet::new();
    let mut compiled=BTreeSet::new();
    if !c.source_projection_unchanged(){
        source.insert("SOURCE_PROJECTION_DRIFT");
        compiled.insert("SOURCE_PROJECTION_DRIFT");
    }
    if c.policy.real_execution_authorized || c.policy.real_daily_trades!=0{
        source.insert("REAL_POLICY_NOT_DENIED");
        compiled.insert("REAL_POLICY_NOT_DENIED");
    }
    if toggle(c,"general","trade_enabled")!=Some(false){
        source.insert("SOURCE_REQUIRES_GENERAL_TRADE_DISABLED");
        compiled.insert("COMPILED_REQUIRES_GENERAL_TRADE_DISABLED");
    }
    if verify_unwired_source_r174(c).is_err(){
        source.insert("SOURCE_R174_UNWIRED_FROZEN_OR_SWITCH_CONFLICT");
    }
    for group in ["adaptive_entry","dynamic_entry","scale_in_hints","analytics"]{
        if toggle(c,group,"enabled")!=Some(false){
            source.insert(match group{
                "adaptive_entry"=>"SOURCE_ADAPTIVE_ENTRY_UNSUPPORTED",
                "dynamic_entry"=>"SOURCE_DYNAMIC_ENTRY_UNSUPPORTED",
                "scale_in_hints"=>"SOURCE_SCALE_IN_UNSUPPORTED",
                _=>"SOURCE_ANALYTICS_UNSUPPORTED",
            });
        }
    }
    if toggle(c,"velocity","enabled")!=Some(true){
        source.insert("SOURCE_REQUIRES_VELOCITY_ON");
    }
    if toggle(c,"general","paper_entries_enabled")!=Some(true){
        source.insert("SOURCE_REQUIRES_PAPER_ENTRIES_ENABLED");
        compiled.insert("COMPILED_REQUIRES_PAPER_ENTRIES_ENABLED");
    }
    if toggle(c,"real_trial","enabled")!=Some(false){
        compiled.insert("COMPILED_REQUIRES_REAL_TRIAL_DISABLED");
    }
    if toggle(c,"real_trial","autonomous_real_enabled")!=Some(false){
        compiled.insert("COMPILED_REQUIRES_AUTONOMOUS_REAL_DISABLED");
    }
    if c.policy.paper_daily_trades!=MAX_DAILY_PAPER as u64{
        compiled.insert("COMPILED_PAPER_DAILY_DIFF_FROM_500");
    }
    if c.policy.general_max_positions!=MAX_CONCURRENT_PAPER as u64{
        compiled.insert("COMPILED_PAPER_CONCURRENT_QUOTA_DIFF");
    }
    if verify_public_paper_unwired_runtime_features(c).is_err(){
        compiled.insert("COMPILED_R95_FEATURE_PROFILE_UNSUPPORTED");
    }
    if verify_paper_portfolio_quota_parity(c).is_err(){
        compiled.insert("COMPILED_R77_QUOTA_OR_POLICY_DENIED");
    }
    // These ARE the existing constructors/admissions, not guessed acceptance.
    let source_constructor_accepted =
        SupervisedStrategyPaper::new(c.clone(),at_wall_ms).is_ok();
    let compiled_admission_accepted=admit_public_paper_yaml_review(c).is_ok();
    if source_constructor_accepted!=source.is_empty(){
        source.insert("SOURCE_DIAGNOSTIC_NOT_EXHAUSTIVE");
    }
    if compiled_admission_accepted!=compiled.is_empty(){
        compiled.insert("COMPILED_DIAGNOSTIC_NOT_EXHAUSTIVE");
    }
    json!({
        "schema":"SPOTARB/R258/DUAL-PAPER-ADMISSION-DIAGNOSTIC/V1",
        "source_yaml_sha256":c.sha256,
        "source_paper_r155":{
            "preflight_blockers":source,
            "actual_in_memory_constructor_accepted":source_constructor_accepted,
            "installed_or_running":false
        },
        "compiled_fallback_r95":{
            "preflight_blockers":compiled,
            "actual_offline_yaml_admission_accepted":compiled_admission_accepted,
            "installed_or_running":false
        },
        "source_paper_daily_requested":c.policy.paper_daily_trades,
        "compiled_paper_daily":MAX_DAILY_PAPER,
        "compiled_concurrent":MAX_CONCURRENT_PAPER,
        "original_yaml_changed":false,
        "tested_research_only":true,
        "installed_field_applications_proven":0,
        "public_ws_started":false,
        "private_exchange_api_used":false,
        "real_orders_authorized":false,
        "automatic_cutover_authorized":false
    })
}

#[cfg(test)]
mod tests{
    use super::*;
    fn original()->NativeTradingConfig{
        NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap()
    }
    const WHEN:i64=1_792_666_000_000;
    #[test]fn r258_original_operator_yaml_is_denied_in_both_distinct_paper_profiles(){
        let r=review_two_paper_admissions_r258(&original(),WHEN);
        assert_eq!(r["source_paper_r155"]["actual_in_memory_constructor_accepted"],false);
        assert_eq!(r["compiled_fallback_r95"]["actual_offline_yaml_admission_accepted"],false);
        let source=r["source_paper_r155"]["preflight_blockers"].as_array().unwrap();
        let compiled=r["compiled_fallback_r95"]["preflight_blockers"].as_array().unwrap();
        assert_eq!(source.iter().any(|x|x=="SOURCE_REQUIRES_GENERAL_TRADE_DISABLED"),original().raw["general"]["trade_enabled"]==true);
        for b in ["SOURCE_SCALE_IN_UNSUPPORTED","SOURCE_ANALYTICS_UNSUPPORTED"]{
            assert!(source.iter().any(|x|x==b),"{b}");
        }
        for b in ["COMPILED_PAPER_DAILY_DIFF_FROM_500",
                  "COMPILED_REQUIRES_PAPER_ENTRIES_ENABLED"]{
            assert!(compiled.iter().any(|x|x==b),"{b}");
        }
        assert_eq!(compiled.iter().any(|x|x=="COMPILED_REQUIRES_REAL_TRIAL_DISABLED"),original().raw["real_trial"]["enabled"]==true);
        assert_eq!(r["real_orders_authorized"],false);
    }
    #[test]fn r258_source_research_can_accept_isolated_restricted_yaml_without_deploy(){
        let raw=include_str!("../config/native_trading.yaml")
            .replacen("trade_enabled: true","trade_enabled: false",1)
            .replacen("paper_entries_enabled: false","paper_entries_enabled: true",1)
            .replacen("scale_in_hints:\n  enabled: true",
                      "scale_in_hints:\n  enabled: false",1)
            .replacen("analytics:\n  enabled: true",
                      "analytics:\n  enabled: false",1);
        let cfg=NativeTradingConfig::parse(&raw).unwrap();
        let r=review_two_paper_admissions_r258(&cfg,WHEN);
        assert_eq!(r["source_paper_r155"]["actual_in_memory_constructor_accepted"],true);
        assert_eq!(r["source_paper_r155"]["preflight_blockers"],json!([]));
        assert_eq!(r["compiled_fallback_r95"]["actual_offline_yaml_admission_accepted"],false);
        assert_eq!(r["source_paper_daily_requested"],3000);
        assert_eq!(r["automatic_cutover_authorized"],false);
    }
    #[test]fn r258_mutated_parsed_raw_never_earns_fictitious_admission(){
        let mut cfg=original();
        cfg.raw["analytics"]["enabled"]=serde_yaml::Value::Bool(false);
        let r=review_two_paper_admissions_r258(&cfg,WHEN);
        assert_eq!(r["source_paper_r155"]["actual_in_memory_constructor_accepted"],false);
        assert_eq!(r["compiled_fallback_r95"]["actual_offline_yaml_admission_accepted"],false);
        assert!(r["source_paper_r155"]["preflight_blockers"].as_array().unwrap()
            .iter().any(|x|x=="SOURCE_PROJECTION_DRIFT"));
    }
}