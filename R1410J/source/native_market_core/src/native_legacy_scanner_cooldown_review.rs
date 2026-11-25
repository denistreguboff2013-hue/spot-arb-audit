//! R121 / Astra P1-06: source-mapped, research-only Python cooldown stages.
//! No private exchange proof, live callers, or permission to send orders.
//! Source: /home/tmpadmin/spotbot/strategy.py, inspected 17 Oct 2026.
//! R120 already stamps successful modeled PAPER opens in the portfolio.
//! This adapter is NOT wired to the deployed PAPER service.
use crate::native_paper_portfolio_review::PaperPortfolio;

#[derive(Clone,Copy,Debug,Eq,PartialEq)]
pub enum LegacyCooldownStage{
 MissingFuturesSymbol, FuturesSymbolLookupError,
 PreEntryGateRejected, PreEntryGateError,
 PaperOpenCommitted, OpenOrphanManual, OpenFailure,
 EntryVerificationCleaned, EntryVerificationManual,
 ProtectiveStopCleaned, ProtectiveStopManual,
 RealOpenCommitted, CloseTruthUnknown,
 CloseTruthConfirmedFlat, CloseTruthOrphanFuturesManual,
 RejectedInsufficientNet, RejectedBlacklist, RejectedNoSignal,
 CloseTruthOrphanSpotContinue,
}
impl LegacyCooldownStage{
 /// Exact legacy _mark_cooldown call line, or None when source does NOT mark.
 pub fn source_mark_line(self)->Option<u32>{
  use LegacyCooldownStage::*;
  Some(match self{
   MissingFuturesSymbol=>4103,
   FuturesSymbolLookupError=>4112,
   PreEntryGateRejected=>4294,
   PreEntryGateError=>4300,
   PaperOpenCommitted=>4505,
   OpenOrphanManual=>4619,
   OpenFailure=>4624,
   EntryVerificationCleaned=>4816,
   EntryVerificationManual=>4845,
   ProtectiveStopCleaned=>4927,
   ProtectiveStopManual=>4957,
   RealOpenCommitted=>4994,
   CloseTruthUnknown=>5344,
   CloseTruthConfirmedFlat=>5470,
   CloseTruthOrphanFuturesManual=>5547,
   RejectedInsufficientNet|RejectedBlacklist|RejectedNoSignal|
   CloseTruthOrphanSpotContinue=>return None,
  })
 }
}
#[derive(Clone,Copy,Debug,Eq,PartialEq)]
pub enum CooldownReviewError{InvalidBase,UnreconciledClock,ClockRollback}
/// Offline stage preview only; stage must originate in a separately trusted
/// execution/close FSM. Source labels are NOT proof an exchange event happened.
/// Never rewrites the position, close timestamps, or daily counts.
pub fn preview_record_scanner_stage(
 p:&mut PaperPortfolio,base:&str,now_monotonic_ns:u64,
 stage:LegacyCooldownStage
)->Result<bool,CooldownReviewError>{
 if stage.source_mark_line().is_none(){return Ok(false)}
 if base.is_empty()||base.trim()!=base || now_monotonic_ns==0{
  return Err(CooldownReviewError::InvalidBase)
 }
 if p.cold_boot_unknown{return Err(CooldownReviewError::UnreconciledClock)}
 if p.scan_last_mark_ns.get(base).is_some_and(|last|now_monotonic_ns<*last){
  return Err(CooldownReviewError::ClockRollback)
 }
 p.scan_last_mark_ns.insert(base.into(),now_monotonic_ns);
 Ok(true)
}

#[cfg(test)]
mod tests{
 use super::*;
 use std::collections::BTreeSet;
 #[test]fn r121_all_fifteen_python_mark_callsites_mapped_without_duplicates(){
  use LegacyCooldownStage::*;
  let stages=[MissingFuturesSymbol,FuturesSymbolLookupError,
   PreEntryGateRejected,PreEntryGateError,PaperOpenCommitted,
   OpenOrphanManual,OpenFailure,EntryVerificationCleaned,
   EntryVerificationManual,ProtectiveStopCleaned,ProtectiveStopManual,
   RealOpenCommitted,CloseTruthUnknown,CloseTruthConfirmedFlat,
   CloseTruthOrphanFuturesManual];
  let lines:Vec<_>=stages.iter().map(|s|s.source_mark_line().unwrap()).collect();
  assert_eq!(lines,vec![4103,4112,4294,4300,4505,4619,4624,4816,
    4845,4927,4957,4994,5344,5470,5547]);
  assert_eq!(lines.iter().collect::<BTreeSet<_>>().len(),15);
  for stage in [RejectedInsufficientNet,RejectedBlacklist,RejectedNoSignal,
   CloseTruthOrphanSpotContinue]{assert_eq!(stage.source_mark_line(),None)}
 }
 #[test]fn r121_rejected_candidate_never_marks_and_manual_close_keeps_position(){
  let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
  p.positions.insert("X".into(),
    crate::native_paper_market_execution_review::PaperMachine::new(p.assumptions.clone()).unwrap());
  let previous_positions=serde_json::to_value(&p.positions).unwrap();
  assert!(!preview_record_scanner_stage(&mut p,"X",123,
   LegacyCooldownStage::RejectedInsufficientNet).unwrap());
  assert!(p.scan_last_mark_ns.is_empty());
  assert!(preview_record_scanner_stage(&mut p,"X",123,
   LegacyCooldownStage::CloseTruthUnknown).unwrap());
  assert_eq!(p.scan_last_mark_ns["X"],123);
  assert_eq!(serde_json::to_value(&p.positions).unwrap(),previous_positions);
  assert_eq!(p.stats.entries,0);
 }
 #[test]fn r121_cold_or_rewound_timestamp_never_creates_mark(){
  let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
  assert!(preview_record_scanner_stage(&mut p,"X",100,
    LegacyCooldownStage::OpenFailure).unwrap());
  assert_eq!(preview_record_scanner_stage(&mut p,"X",99,
    LegacyCooldownStage::OpenFailure),Err(CooldownReviewError::ClockRollback));
  assert_eq!(p.scan_last_mark_ns["X"],100);
  let raw=serde_json::to_string(&p).unwrap();
  let mut cold:PaperPortfolio=serde_json::from_str(&raw).unwrap();
  assert!(cold.cold_boot_unknown);
  assert_eq!(preview_record_scanner_stage(&mut cold,"X",101,
    LegacyCooldownStage::RealOpenCommitted),Err(CooldownReviewError::UnreconciledClock));
  assert_eq!(cold.scan_last_mark_ns["X"],100);
  assert_eq!(preview_record_scanner_stage(&mut p,"",111,
    LegacyCooldownStage::OpenFailure),Err(CooldownReviewError::InvalidBase));
 }
}
