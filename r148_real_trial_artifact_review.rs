//! R148 isolated source-only research harness, NOT wired into live library.
//! Keeps production Rust PAPER unchanged while testing a new policy reader.
pub use spotarb_native_market_core::native_trading_config;
#[path = "../src/native_real_trial_artifact_policy_review.rs"]
mod original_real_trial_research;
