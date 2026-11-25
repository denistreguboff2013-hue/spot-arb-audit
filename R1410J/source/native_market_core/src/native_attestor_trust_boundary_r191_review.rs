// R191: threat-model prerequisites for a REAL independent witness.
// This is a mandatory gate checklist, NOT an attestor deployment.
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum AttestorDomainR191{
 SameProcessMock,SameHostSeparateProcess,IndependentMachine,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub struct RemoteTrustRequirementsR191{
 pub domain:AttestorDomainR191,
 pub monotonic_durable_external_store:bool,
 pub signing_key_remote_only:bool,
 pub verifier_pinned_remote_identity:bool,
 pub challenge_is_fresh:bool,
 pub owner_generation_anchor_bound:bool,
 pub authenticated_recovery:bool,
 pub power_loss_and_partition_tested:bool,
}
impl RemoteTrustRequirementsR191{
 pub fn current_mock()->Self{
  Self{domain:AttestorDomainR191::SameProcessMock,
   monotonic_durable_external_store:false,signing_key_remote_only:false,
   verifier_pinned_remote_identity:false,challenge_is_fresh:true,
   owner_generation_anchor_bound:true,authenticated_recovery:false,
   power_loss_and_partition_tested:false}
 }
 pub fn production_independence_proven(&self)->bool{
  self.domain==AttestorDomainR191::IndependentMachine
   &&self.monotonic_durable_external_store
   &&self.signing_key_remote_only
   &&self.verifier_pinned_remote_identity
   &&self.challenge_is_fresh
   &&self.owner_generation_anchor_bound
   &&self.authenticated_recovery
   &&self.power_loss_and_partition_tested
 }
}
#[cfg(test)]
mod r191_tests{
 use super::*;
 #[test]fn r191_mock_and_same_host_never_qualify_as_independent(){
  let mock=RemoteTrustRequirementsR191::current_mock();
  assert!(!mock.production_independence_proven());
  let same_host=RemoteTrustRequirementsR191{
   domain:AttestorDomainR191::SameHostSeparateProcess,
   monotonic_durable_external_store:true,signing_key_remote_only:true,
   verifier_pinned_remote_identity:true,challenge_is_fresh:true,
   owner_generation_anchor_bound:true,authenticated_recovery:true,
   power_loss_and_partition_tested:true,
  };
  assert!(!same_host.production_independence_proven());
  let remote=RemoteTrustRequirementsR191{
   domain:AttestorDomainR191::IndependentMachine,..same_host
  };
  assert!(remote.production_independence_proven());
  assert!(!RemoteTrustRequirementsR191{
   signing_key_remote_only:false,..remote
  }.production_independence_proven());
 }
}
