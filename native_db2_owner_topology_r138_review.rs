//! Astra R138: read-only service topology evidence for DB1=>DB2.
//! A stopped legacy mirror is NOT certified by an old SYNCED status.
//! Active PAPER's separate working directory is not automatically a DB2 replacement.
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub struct ServiceObservation{
 pub active:bool,pub enabled:bool,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub struct Db2TopologyObservation{
 pub legacy_scanner:ServiceObservation,
 pub legacy_mirror:ServiceObservation,
 pub native_paper:ServiceObservation,
 pub mirror_status_is_fresh:bool,
 pub source_database_is_fresh:bool,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum Db2TopologyClass{
 DormantLegacyMirrorSeparatePaper,
 DormantLegacyMirrorNoOwner,
 MirrorRunningUncertified,
 MirrorOwnerConflict,
 InsufficientEvidence,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub struct Db2TopologyReview{
 pub class:Db2TopologyClass,
 pub proves_active_mirror:bool,
 pub proves_native_replacement:bool,
 pub may_authorize_trading:bool,
}
pub fn review_topology(v:Db2TopologyObservation)->Db2TopologyReview{
 let class=if !v.legacy_mirror.active&&!v.legacy_mirror.enabled{
  if v.native_paper.active{
   Db2TopologyClass::DormantLegacyMirrorSeparatePaper
  }else{Db2TopologyClass::DormantLegacyMirrorNoOwner}
 }else if v.legacy_mirror.active&&v.native_paper.active&&v.legacy_scanner.active{
  Db2TopologyClass::MirrorOwnerConflict
 }else if v.legacy_mirror.active&&v.legacy_scanner.active
  &&v.mirror_status_is_fresh&&v.source_database_is_fresh{
  Db2TopologyClass::MirrorRunningUncertified
 }else{Db2TopologyClass::InsufficientEvidence};
 Db2TopologyReview{class,proves_active_mirror:false,
   proves_native_replacement:false,may_authorize_trading:false}
}
#[cfg(test)]
mod tests{
 use super::*;
 fn svc(active:bool,enabled:bool)->ServiceObservation{
  ServiceObservation{active,enabled}
 }
 #[test]fn r138_real_observed_disabled_legacy_is_not_db2_sync(){
  let result=review_topology(Db2TopologyObservation{
   legacy_scanner:svc(false,false),legacy_mirror:svc(false,false),
   native_paper:svc(true,true),mirror_status_is_fresh:false,
   source_database_is_fresh:false});
  assert_eq!(result.class,Db2TopologyClass::DormantLegacyMirrorSeparatePaper);
  assert!(!result.proves_active_mirror&&!result.proves_native_replacement
   &&!result.may_authorize_trading);
 }
 #[test]fn r138_stale_old_sync_not_live_even_if_scanner_running(){
  let review=review_topology(Db2TopologyObservation{
   legacy_scanner:svc(true,true),legacy_mirror:svc(true,true),
   native_paper:svc(false,false),mirror_status_is_fresh:false,
   source_database_is_fresh:true});
  assert_eq!(review.class,Db2TopologyClass::InsufficientEvidence);
  assert!(!review.proves_active_mirror);
 }
 #[test]fn r138_even_active_fresh_mirror_does_not_certify_replacement(){
  let review=review_topology(Db2TopologyObservation{
   legacy_scanner:svc(true,true),legacy_mirror:svc(true,true),
   native_paper:svc(false,false),mirror_status_is_fresh:true,
   source_database_is_fresh:true});
  assert_eq!(review.class,Db2TopologyClass::MirrorRunningUncertified);
  assert!(!review.proves_active_mirror&&!review.proves_native_replacement);
 }
}
