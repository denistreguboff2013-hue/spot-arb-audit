//! P0-02 read-only public virtual PAPER vs durable Native PaperOwner
//! parity gate. This DOES NOT turn book-modeled fills into exchange proofs.
use crate::{
  native_paper_portfolio_review::PaperPortfolio,
  native_paper_market_execution_review::PaperPhase,
  native_paper_durable_journal_review::RecoveredPaper,
};
use serde_json::{json,Value};
use std::path::Path;
#[derive(Debug,PartialEq,Eq)]
pub enum AlignmentError{
    OwnerHistoryNotSafe,
    ClosingLegsNotAttested,
    DifferentSession,
    DifferentPortfolioCounts,
    DifferentVirtualPosition,
    MissingNativeFillOwner,
    MissingOrCorruptNativeJournal,
}
#[derive(Debug,PartialEq,Eq)]
pub struct OwnerParityDiagnostic{
    pub public_virtual_book_matches_local_owner:bool,
    pub owner_recovery_quarantined:bool,
    pub authenticated_exchange_fills:bool,
    pub complete_four_leg_close_attested:bool,
    pub may_resume_or_trade_real:bool,
}
/// Read the durable Native owner journal itself; never trust caller-built
/// RecoveredPaper as independently sourced provenance. Still diagnostic only.
pub fn inspect_virtual_paper_from_native_journal(
    public_book:&PaperPortfolio,owner_journal:&Path,
)->Result<OwnerParityDiagnostic,AlignmentError>{
    let cold=crate::native_paper_durable_journal_review::read_journal(owner_journal)
        .map_err(|_|AlignmentError::MissingOrCorruptNativeJournal)?
        .ok_or(AlignmentError::MissingOrCorruptNativeJournal)?;
    inspect_virtual_paper_vs_durable_owner(public_book,&cold)
}
pub fn inspect_virtual_paper_vs_durable_owner(
    p:&PaperPortfolio,r:&RecoveredPaper,
)->Result<OwnerParityDiagnostic,AlignmentError>{
    if r.may_resume_paper||r.may_trade_real||
       r.state.real_trading_authorized||r.sequence==0{
        return Err(AlignmentError::OwnerHistoryNotSafe)
    }
    // v1 Native PaperOwner deliberately has NO source-shaped durable legs
    // 3/4. Never "reconcile" public-book virtual closes or paper profits.
    if p.stats.closed!=0||p.stats.modeled_realized_usdt!=0.0||
       r.state.closed!=0||r.state.modeled_pnl_usdt!=0.0{
        return Err(AlignmentError::ClosingLegsNotAttested)
    }
    if p.day_yek!=r.state.day_yek||
       p.started_wall_ms!=r.state.started_wall_ms{
        return Err(AlignmentError::DifferentSession)
    }
    if p.stats.entries!=r.state.entries||
       p.positions.len()!=r.state.positions.len(){
        return Err(AlignmentError::DifferentPortfolioCounts)
    }
    if r.pending_reconciliation.len()!=p.positions.len()||
       r.pending_reconciliation.iter().any(|base|!p.positions.contains_key(base)){
        return Err(AlignmentError::OwnerHistoryNotSafe)
    }
    for (base,model) in &p.positions{
        let owned=r.state.positions.get(base)
            .ok_or(AlignmentError::MissingNativeFillOwner)?;
        let (phase,expected):( &str,Value)=match &model.phase{
            PaperPhase::AwaitingHedge(pos)=>("AWAITING_HEDGE",json!(pos)),
            PaperPhase::Hedged(pos)=>("HEDGED",json!(pos)),
            PaperPhase::AwaitingSpotExit{pos,cover_quote,cover_ns,close_estimate_quote}=>
                ("EXIT_PENDING",json!({"position":pos,"cover_quote":cover_quote,
                    "cover_ns":cover_ns,"close_estimate_quote":close_estimate_quote})),
            _=>return Err(AlignmentError::DifferentVirtualPosition),
        };
        if owned.base!=*base||owned.phase!=phase||
           owned.paper_details!=expected{
            return Err(AlignmentError::DifferentVirtualPosition)
        }
        // A matching virtual machine is only the *model side* of parity;
        // the separately validated Native owner maintains its own fill
        // sources and quarantine, and this function cannot mint them.
        if !owned.quarantine||!owned.fills.restart_quarantine||
          owned.fills.exchange_truth_authenticated||
          owned.fills.may_send_real_or_cancel{
            return Err(AlignmentError::OwnerHistoryNotSafe)
        }
    }
    Ok(OwnerParityDiagnostic{
       public_virtual_book_matches_local_owner:true,
       owner_recovery_quarantined:true,
       authenticated_exchange_fills:false,
       complete_four_leg_close_attested:false,
       may_resume_or_trade_real:false,
    })
}
#[cfg(test)]
mod tests{
    use super::*;
    use crate::native_paper_owner_transaction_review::PaperOwner;
    #[test]
    fn actual_native_journal_nonempty_public_model_matches_without_fill_authority(){
        use crate::{
            native_paper_market_execution_review::{
                PaperAssumptions,PaperMachine,PaperPhase,PaperPosition,
            },
            native_paper_partial_recovery_gate::PaperPartialGate,
            fill_economics::decimal_to_atoms,
        };
        let folder=std::env::temp_dir().join(format!(
            "astra_native_journal_nonempty_{}_{}",
            std::process::id(),crate::transport::monotonic_ns()));
        std::fs::create_dir_all(&folder).unwrap();
        let path=folder.join("owner.jsonl");
        let mut owner=PaperOwner::start_new(&path,1_791_786_000_000).unwrap();
        assert_eq!(owner.transact(|book,gates|{
            book.stats.entries=1;
            let pos=PaperPosition{
                base:"ONE".into(),signal_ns:1,spot_buy_ns:1,
                signal_estimated_net_bps:55.0,
                spot_gross_base:4.0,spot_delivered_base:4.0,
                spot_paid_quote:10.0,
                futures_short_ns:None,
                futures_short_proceeds_quote:None,
                futures_short_base:None,
                hedge_entry_estimated_net_bps:None,
            };
            book.positions.insert("ONE".into(),
                PaperMachine{config:PaperAssumptions::default(),
                    phase:PaperPhase::AwaitingHedge(pos)});
            gates.insert("ONE".into(),PaperPartialGate::new(
                7,"owner_one_1","ONEUSDT","spot1","spot_cl1",
                decimal_to_atoms("4").unwrap()).unwrap());
            Ok(())
        }).unwrap(),2);
        let ok=inspect_virtual_paper_from_native_journal(&owner.portfolio,&path).unwrap();
        assert!(ok.public_virtual_book_matches_local_owner);
        assert!(ok.owner_recovery_quarantined);
        assert!(!ok.authenticated_exchange_fills&&!ok.complete_four_leg_close_attested);
        assert!(!ok.may_resume_or_trade_real);
        let mut modified=owner.portfolio.clone();
        if let PaperPhase::AwaitingHedge(ref mut pos)=modified.positions
             .get_mut("ONE").unwrap().phase{
            pos.spot_paid_quote=11.0;
        }
        assert_eq!(inspect_virtual_paper_from_native_journal(&modified,&path),
            Err(AlignmentError::DifferentVirtualPosition));
        assert_eq!(inspect_virtual_paper_from_native_journal(
            &owner.portfolio,&folder.join("nonexistent.jsonl")),
            Err(AlignmentError::MissingOrCorruptNativeJournal));
        let mut broken=std::fs::read(&path).unwrap();
        broken.push(b'{');
        std::fs::write(&path,broken).unwrap();
        assert_eq!(inspect_virtual_paper_from_native_journal(&owner.portfolio,&path),
            Err(AlignmentError::MissingOrCorruptNativeJournal));
        std::fs::remove_dir_all(folder).unwrap();
    }
    #[test]
    fn empty_paper_owner_parity_is_model_only_and_denies_real_rights(){
        let folder=std::env::temp_dir().join(format!(
            "astra_virtual_owner_parity_{}_{}",std::process::id(),
            crate::transport::monotonic_ns()));
        std::fs::create_dir_all(&folder).unwrap();
        let path=folder.join("owner.jsonl");
        let owner=PaperOwner::start_new(&path,1_791_786_000_000).unwrap();
        let cold=PaperOwner::inspect_recovery(&path).unwrap().unwrap();
        let ok=inspect_virtual_paper_vs_durable_owner(&owner.portfolio,&cold).unwrap();
        assert!(ok.public_virtual_book_matches_local_owner);
        assert!(ok.owner_recovery_quarantined);
        assert!(!ok.authenticated_exchange_fills);
        assert!(!ok.complete_four_leg_close_attested);
        assert!(!ok.may_resume_or_trade_real);
        let mut changed=owner.portfolio.clone();
        changed.stats.entries=1;
        assert_eq!(inspect_virtual_paper_vs_durable_owner(&changed,&cold),
                   Err(AlignmentError::DifferentPortfolioCounts));
        changed=owner.portfolio.clone();
        changed.stats.closed=1;changed.stats.entries=1;
        assert_eq!(inspect_virtual_paper_vs_durable_owner(&changed,&cold),
                   Err(AlignmentError::ClosingLegsNotAttested));
        std::fs::remove_dir_all(folder).unwrap();
    }
}
