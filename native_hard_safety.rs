//! R32D6C1 — independent review-build mutation egress HARD STOP.
//!
//! The native exchange execution path is not production-certified. For all
//! ordinary library/binary/integration-test builds this is a compile-time
//! DENIAL, independent of caller-supplied state, generation, credentials,
//! exchange snapshots, environment variables, executor implementation,
//! or even syntactically valid previously prepared network mutation intents.
//!
//! Only INTERNAL Rust unit-test builds may simulate mutation dispatch into
//! fake HttpExecutor implementations. The real HttpsClient still blocks every
//! mutation in unit tests as well. There is no runtime "enable" parameter.
//!
//! IMPORTANT: This is review egress containment, NOT a production trading
//! admission policy, not a full kill-switch implementation, and not a
//! certificate of actual exchange state.
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum NativeHardSafetyError{
    /// No native production mutation capability has been certified.
    MutationCapabilityNotCertified,
}

/// Central, non-configurable R32D6C1 review egress policy.
///
/// Call from EVERY native mutation entrypoint before invoking HttpExecutor,
/// and from the internal with_authorized_{entry,close}_send lock boundaries.
#[inline]
pub fn require_native_mutation_egress()->Result<(),NativeHardSafetyError>{
    #[cfg(test)]
    {
        // Existing unit fixtures emulate the proposed durable pre-send path.
        // This code is not compiled into ordinary binaries or into the
        // library linked from integration tests.
        Ok(())
    }
    #[cfg(not(test))]
    {
        Err(NativeHardSafetyError::MutationCapabilityNotCertified)
    }
}

#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn r32d6c1_only_internal_unit_fixtures_emulate_send(){
        assert_eq!(require_native_mutation_egress(),Ok(()));
    }
}
