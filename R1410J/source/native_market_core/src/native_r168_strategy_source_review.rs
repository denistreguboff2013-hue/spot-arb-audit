//! R168 P1-06 source mutation to *diagnostic* Rust decisions (not installed PAPER).
//! Tests reuse R97/R98/R99 consumers; no network, storage, exchange or order API.
#[cfg(test)]
mod tests {
    use crate::native_trading_config::NativeTradingConfig;
    use crate::native_paper_model_config_review::inspect_dynamic_exit_from_yaml;
    use crate::native_legacy_strategy_policy_review::{
        inspect_dynamic_entry_target_readonly, inspect_legacy_close_readonly,
        inspect_scanner_min_depth_readonly, LegacyCloseObservation,
    };
    use crate::native_legacy_adaptive_entry_review::{
        inspect_adaptive_entry_readonly, AdaptiveTelemetry, AdaptiveAdvice,
    };
    use crate::position_lifecycle::CloseDecision;
    const BASE: &str = include_str!("../config/native_trading_operator_baseline_r64.yaml");

    fn config(yaml: &str) -> NativeTradingConfig {
        NativeTradingConfig::parse(yaml).expect("typed source YAML")
    }
    // Change exactly ONE leaf under the selected root section; preserve all other
    // lines, including operator profile differences, for a unique-path mutation.
    fn changed(src: &str, section: &str, key: &str, value: &str, segment: bool) -> String {
        let header = format!("{section}:");
        let prefix = format!("{}{key}:", if segment { "    " } else { "  " });
        let mut active = false;
        let mut count = 0;
        let mut text = String::new();
        for line in src.lines() {
            if !line.starts_with(' ') && !line.starts_with('#') && line.ends_with(':') {
                active = line == header;
            }
            if active && line.starts_with(&prefix) {
                text.push_str(&format!("{prefix} {value}\n"));
                count += 1;
            } else {
                text.push_str(line);
                text.push('\n');
            }
        }
        assert_eq!(count, 1, "must mutate unique YAML path {section}.{key}");
        assert_ne!(text, src);
        text
    }
    fn scalar(section: &str, key: &str, value: &str) -> NativeTradingConfig {
        config(&changed(BASE, section, key, value, false))
    }
    fn segment(section: &str, key: &str, value: &str) -> NativeTradingConfig {
        config(&changed(BASE, section, key, value, true))
    }
    fn observation() -> LegacyCloseObservation {
        LegacyCloseObservation {
            observed_spread_bps: 40.0,
            executable_close_spread_bps: Some(0.2),
            velocity_bps_s: Some(0.0),
            elapsed_seconds: 25,
            position_units: 100,
            force_close: false,
            futures_emergency_stop_hit: false,
        }
    }
    fn adaptive_src() -> String {
        changed(BASE, "adaptive_entry", "enabled", "true", false)
    }
    fn telemetry(depth: f64, trade_cap: f64, slip: Option<f64>) -> AdaptiveTelemetry {
        AdaptiveTelemetry {
            maximum_entry_usdt: depth,
            maximum_trade_entry_usdt: trade_cap,
            age_seconds: Some(5.0),
            trade_slippage_bps: slip,
        }
    }
    fn advice(cfg: &NativeTradingConfig, depth: f64, trade_cap: f64,
              slip: Option<f64>) -> AdaptiveAdvice {
        inspect_adaptive_entry_readonly(cfg, telemetry(depth, trade_cap, slip))
            .expect("diagnostic") .expect("enabled")
    }

    #[test]
    fn r168_01_dynamic_enabled_changes_min_lifetime() {
        let on = inspect_dynamic_exit_from_yaml(&config(BASE), 50_017.5).unwrap();
        let off = inspect_dynamic_exit_from_yaml(
            &scalar("dynamic", "enabled", "false"), 50_017.5).unwrap();
        assert_eq!(on, (0.5, 208)); assert_eq!(off, (0.5, 15));
    }
    #[test]
    fn r168_02_dynamic_segments_change_exit_threshold() {
        let old = inspect_dynamic_exit_from_yaml(&config(BASE), 50_017.5).unwrap();
        let new = inspect_dynamic_exit_from_yaml(
            &segment("dynamic", "exit_to_bps", "1.0"), 50_017.5).unwrap();
        assert_eq!(old.0, 0.5); assert_eq!(new.0, 0.75);
    }
    #[test]
    fn r168_03_dynamic_entry_enabled_changes_target() {
        let off = inspect_dynamic_entry_target_readonly(&config(BASE), 35.0).unwrap();
        let on = inspect_dynamic_entry_target_readonly(
            &scalar("dynamic_entry", "enabled", "true"), 35.0).unwrap();
        assert_eq!(off, 12.0); assert_eq!(on, 40.0);
    }
    #[test]
    fn r168_04_dynamic_entry_segment_changes_target() {
        let on_src = changed(BASE, "dynamic_entry", "enabled", "true", false);
        let before = inspect_dynamic_entry_target_readonly(&config(&on_src), 35.0).unwrap();
        let after = inspect_dynamic_entry_target_readonly(
            &config(&changed(&on_src, "dynamic_entry", "target_from_bps", "60", true)),
            35.0).unwrap();
        assert_eq!(before, 40.0); assert_eq!(after, 60.0);
    }
    #[test]
    fn r168_05_velocity_hold_changes_close_to_hold() {
        let mut o = observation(); o.velocity_bps_s = Some(0.1);
        let hold = inspect_legacy_close_readonly(&config(BASE), o).unwrap();
        let close = inspect_legacy_close_readonly(
            &scalar("velocity", "hold_pos_bps_s", "0.2"), o).unwrap();
        assert_eq!(hold.decision, CloseDecision::Hold);
        assert_eq!(close.decision, CloseDecision::CloseNormal);
    }
    #[test]
    fn r168_06_velocity_emergency_neg_changes_trigger() {
        let mut o = observation(); o.velocity_bps_s = Some(-7.0);
        o.observed_spread_bps = 20.0;
        let emergency = inspect_legacy_close_readonly(&config(BASE), o).unwrap();
        let normal = inspect_legacy_close_readonly(
            &scalar("velocity", "emergency_neg_bps_s", "-8.0"), o).unwrap();
        assert_eq!(emergency.decision, CloseDecision::CloseEmergency);
        assert_eq!(normal.decision, CloseDecision::CloseNormal);
    }
    #[test]
    fn r168_07_velocity_emergency_spread_changes_trigger() {
        let mut o = observation(); o.observed_spread_bps = -51.0;
        let emergency = inspect_legacy_close_readonly(&config(BASE), o).unwrap();
        let normal = inspect_legacy_close_readonly(
            &scalar("velocity", "emergency_spread_bps", "-60.0"), o).unwrap();
        assert_eq!(emergency.decision, CloseDecision::CloseEmergency);
        assert_eq!(normal.decision, CloseDecision::CloseNormal);
    }
    #[test]
    fn r168_08_velocity_guard_changes_trigger() {
        let mut o = observation(); o.velocity_bps_s = Some(-7.0);
        o.observed_spread_bps = 20.0;
        let emergency = inspect_legacy_close_readonly(&config(BASE), o).unwrap();
        let normal = inspect_legacy_close_readonly(
            &scalar("velocity", "emergency_guard_spread_bps", "15.0"), o).unwrap();
        assert_eq!(emergency.decision, CloseDecision::CloseEmergency);
        assert_eq!(normal.decision, CloseDecision::CloseNormal);
    }
    #[test]
    fn r168_09_adaptive_enabled_changes_none_to_sizing() {
        assert!(inspect_adaptive_entry_readonly(
            &config(BASE), telemetry(170.0, 90.0, Some(1.0))).unwrap().is_none());
        let enabled = config(&adaptive_src());
        let result = advice(&enabled, 170.0, 90.0, Some(1.0));
        assert_eq!((result.wanted_usdt, result.applied_usdt), (170.0, 90.0));
        assert!(!result.can_trade && !result.exchange_authenticated);
    }
    #[test]
    fn r168_10_adaptive_min_usdt_changes_optional_trade_cap() {
        let on_src = adaptive_src();
        let old = advice(&config(&on_src), 170.0, 90.0, Some(1.0));
        let new = advice(&config(&changed(&on_src, "adaptive_entry", "min_usdt", "100", false)),
            170.0, 90.0, Some(1.0));
        assert_eq!(old.applied_usdt, 90.0); assert_eq!(new.applied_usdt, 170.0);
    }
    #[test]
    fn r168_11_adaptive_max_usdt_changes_wanted() {
        let on_src = adaptive_src();
        let old = advice(&config(&on_src), 170.0, 90.0, Some(1.0));
        let new = advice(&config(&changed(&on_src, "adaptive_entry", "max_usdt", "120", false)),
            170.0, 90.0, Some(1.0));
        assert_eq!(old.wanted_usdt, 170.0); assert_eq!(new.wanted_usdt, 120.0);
        assert_eq!(new.applied_usdt, 90.0);
    }
    #[test]
    fn r168_12_adaptive_step_usdt_changes_quantization() {
        let on_src = adaptive_src();
        let old = advice(&config(&on_src), 170.0, 90.0, Some(1.0));
        let new = advice(&config(&changed(&on_src, "adaptive_entry", "step_usdt", "30", false)),
            170.0, 90.0, Some(1.0));
        assert_eq!(old.wanted_usdt, 170.0); assert_eq!(new.wanted_usdt, 150.0);
    }
    #[test]
    fn r168_13_adaptive_slip_bps_gates_sizing() {
        let on_src = adaptive_src();
        let old = advice(&config(&on_src), 170.0, 90.0, Some(2.0));
        let new = advice(&config(&changed(&on_src, "adaptive_entry", "slip_bps", "1", false)),
            170.0, 90.0, Some(2.0));
        assert_eq!(old.applied_usdt, 90.0); assert_eq!(new.applied_usdt, 0.0);
    }
    #[test]
    fn r168_14_adaptive_require_depth_changes_gate() {
        let on_src = adaptive_src();
        let old = advice(&config(&on_src), 0.0, 0.0, Some(1.0));
        let new = advice(&config(&changed(&on_src, "adaptive_entry",
                 "require_scanner_depth", "false", false)), 0.0, 0.0, Some(1.0));
        assert_eq!(old.applied_usdt, 0.0); assert_eq!(new.applied_usdt, 200.0);
    }
    #[test]
    fn r168_15_adaptive_manual_max_changes_size() {
        let on_src = adaptive_src();
        let old = advice(&config(&on_src), 170.0, 0.0, Some(1.0));
        let new = advice(&config(&changed(&on_src, "adaptive_entry",
                 "max_entry_usdt", "60", false)), 170.0, 0.0, Some(1.0));
        assert_eq!(old.wanted_usdt, 170.0); assert_eq!(new.wanted_usdt, 60.0);
    }
    #[test]
    fn r168_16_scanner_min_depth_changes_admission_threshold() {
        let old = inspect_scanner_min_depth_readonly(&config(BASE)).unwrap();
        let new = inspect_scanner_min_depth_readonly(
            &scalar("scale_in_hints", "min_depth_usdt", "250")).unwrap();
        assert_eq!(old.required_min_trade_usdt, 200.0);
        assert_eq!(new.required_min_trade_usdt, 250.0);
        assert!(!new.is_upsize_signal && !new.may_change_position);
    }
    #[test]
    fn r168_17_invalid_segments_and_in_memory_drift_are_denied() {
        let invalid = config(&changed(BASE, "dynamic", "to_entry_bps", "35", true));
        assert!(inspect_dynamic_exit_from_yaml(&invalid, 35.0).is_err());
        let mut drift = config(BASE);
        drift.raw["velocity"]["emergency_spread_bps"] = serde_yaml::Value::from(-60.0);
        assert!(inspect_legacy_close_readonly(&drift, observation()).is_err());
    }
}
