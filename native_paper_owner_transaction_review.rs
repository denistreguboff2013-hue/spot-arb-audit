//! Astra P0-02: offline durability-before-publish paper portfolio owner.
//! Paper only. No private HTTP, no orders, no automatic crash replay.
use crate::native_paper_portfolio_review::PaperPortfolio;
use crate::native_paper_partial_recovery_gate::PaperPartialGate;
use crate::native_paper_market_execution_review::PaperPhase;
use crate::native_paper_durable_journal_review::{
    append_snapshot_if_prev,append_v2_aligned_snapshot_if_prev,
    read_journal,ReviewSnapshot,JournalError,RecoveredPaper,
};
use std::{collections::BTreeMap,path::{Path,PathBuf}};

#[derive(Debug)]
pub enum OwnerError{
    Journal(JournalError),Quarantine,Invalid(&'static str),
}
impl From<JournalError> for OwnerError{
    fn from(e:JournalError)->Self{Self::Journal(e)}
}
pub struct PaperOwner {
    pub portfolio:PaperPortfolio,
    pub gate_by_base:BTreeMap<String,PaperPartialGate>,
    pub journal:PathBuf,
    pub last_seq:u64,
    pub frozen:bool,
}
impl PaperOwner{
    pub fn start_new(path:&Path,at_wall_ms:i64)->Result<Self,OwnerError>{
        // An existing journal must NEVER silently become a new, flat book.
        if path.exists(){return Err(OwnerError::Quarantine)}
        let portfolio=PaperPortfolio::new(at_wall_ms)
            .map_err(|_|OwnerError::Invalid("invalid session clock"))?;
        let gate_by_base=BTreeMap::new();
        let state=ReviewSnapshot::from_paper(&portfolio,&gate_by_base)?;
        let seq=append_snapshot_if_prev(path,&state,0)?;
        Ok(Self{portfolio,gate_by_base,journal:path.to_path_buf(),
            last_seq:seq,frozen:false})
    }
    /// Propose in a private clone. Persist+fsync the whole after-image,
    /// ONLY THEN publish it to the paper owner in this process.
    pub fn transact<F>(&mut self,mutate:F)->Result<u64,OwnerError>
    where F:FnOnce(&mut PaperPortfolio,
        &mut BTreeMap<String,PaperPartialGate>)->Result<(),OwnerError>{
        self.transact_inner(mutate,None)
    }
    fn transact_inner<F>(&mut self,mutate:F,
        allow_alignment_for:Option<(&str,
          &crate::native_trade2f3_v2_versioned_store::V2ReviewState,
          &crate::native_trade2f3_v2_four_leg_close_review::V2FourLegCloseObservation)>)
        ->Result<u64,OwnerError>
    where F:FnOnce(&mut PaperPortfolio,
        &mut BTreeMap<String,PaperPartialGate>)->Result<(),OwnerError>{
        if self.frozen||self.gate_by_base.values().any(|gate|
            gate.unknown.is_some()||gate.restart_quarantine){
            return Err(OwnerError::Quarantine)
        }
        let mut portfolio=self.portfolio.clone();
        let mut gates=self.gate_by_base.clone();
        mutate(&mut portfolio,&mut gates)?;
        // A caller can mutate public paper structs through transact(); it
        // must NOT be able to fabricate an accepted V2 cross-check marker.
        // Only the source-validating attach_v2_local_alignment route may add it.
        let mut marker_grant_used=false;
        for (name,next) in &gates{
            let prior=self.gate_by_base.get(name)
                .and_then(|g|g.v2_local_alignment.as_ref());
            let now=next.v2_local_alignment.as_ref();
            if now!=prior{
                if prior.is_some()||now.is_none()||
                   allow_alignment_for.map(|(base,_,_)|base)!=Some(name.as_str())
                   ||marker_grant_used{
                    return Err(OwnerError::Quarantine)
                }
                marker_grant_used=true;
            }
        }
        if allow_alignment_for.is_some()&&!marker_grant_used{
            return Err(OwnerError::Quarantine)
        }
        // CAS must advance the ORIGINAL source history, not merely its
        // sequence. Otherwise a caller can replace a previously durable
        // fee-bearing fill ledger with an older internally valid prefix.
        for (base,prior) in &self.gate_by_base{
            if let Some(next)=gates.get(base){
                if !Self::fill_gate_extends(prior,next){
                    return Err(OwnerError::Quarantine)
                }
            }
        }
        let snapshot=ReviewSnapshot::from_paper(&portfolio,&gates)?;
        Self::validate_legs(&portfolio,&gates)?;
        for (base,old_gate) in &self.gate_by_base{
            if old_gate.v2_local_alignment.is_some()&&
                gates.get(base)!=Some(old_gate){
                return Err(OwnerError::Quarantine)
            }
            if old_gate.unknown.is_some()||old_gate.restart_quarantine{
                if gates.get(base)!=Some(old_gate){
                    return Err(OwnerError::Quarantine)
                }
                let old_phase=self.portfolio.positions.get(base).map(|m|
                    format!("{:?}",m.phase));
                let next_phase=portfolio.positions.get(base).map(|m|
                    format!("{:?}",m.phase));
                if old_phase!=next_phase{
                    return Err(OwnerError::Quarantine)
                }
            }
        }
        let saved=match allow_alignment_for{
            Some((base,v2,obs))=>append_v2_aligned_snapshot_if_prev(
                &self.journal,&snapshot,self.last_seq,(base,v2,obs)),
            None=>append_snapshot_if_prev(&self.journal,&snapshot,self.last_seq),
        };
        let seq=match saved{
            Ok(seq)=>seq,
            Err(e)=>{
                // File write can have reached disk before a late fsync error.
                // Stop, don't attempt further mutations with stale RAM.
                self.frozen=true;
                return Err(OwnerError::Journal(e))
            }
        };
        self.portfolio=portfolio;
        self.gate_by_base=gates;
        self.last_seq=seq;
        Ok(seq)
    }
    pub(crate) fn attach_v2_local_alignment(&mut self,base:&str,expected_seq:u64,
        v2:&crate::native_trade2f3_v2_versioned_store::V2ReviewState,
        obs:&crate::native_trade2f3_v2_four_leg_close_review::V2FourLegCloseObservation
    )->Result<u64,OwnerError>{
        if self.frozen||self.last_seq!=expected_seq||
            !matches!(self.portfolio.positions.get(base).map(|x|&x.phase),
                Some(PaperPhase::AwaitingSpotExit{..})){
            return Err(OwnerError::Quarantine)
        }
        let gate=self.gate_by_base.get(base)
            .ok_or(OwnerError::Invalid("missing offline paper owner"))?;
        if gate.v2_local_alignment.is_some(){return Err(OwnerError::Quarantine)}
        let marker=crate::native_paper_v2_closure_bridge_review::
            inspect_paper_v2_alignment(gate,v2,obs,expected_seq)
            .map_err(|_|OwnerError::Invalid("V2 and paper mock sources differ"))?;
        self.transact_inner(|_,g|{
            let owned=g.get_mut(base).ok_or(OwnerError::Quarantine)?;
            if owned.v2_local_alignment.is_some(){return Err(OwnerError::Quarantine)}
            owned.v2_local_alignment=Some(marker);
            Ok(())
        },Some((base,v2,obs)))
    }
    pub fn inspect_recovery(path:&Path)->Result<Option<RecoveredPaper>,OwnerError>{
        read_journal(path).map_err(OwnerError::Journal)
    }
    // Persisted source rows and order identity are an append-only prefix
    // even when the caller has a mutable clone of the whole PAPER owner.
    fn leg_extends(
        old:&crate::fill_economics::LegFillLedger,
        new:&crate::fill_economics::LegFillLedger,
    )->bool{
        old.venue==new.venue&&old.symbol==new.symbol&&old.side==new.side&&
        old.intent_id==new.intent_id&&old.client_order_id==new.client_order_id&&
        old.requested_qty_atoms==new.requested_qty_atoms&&
        old.exchange_order_id.as_ref().map_or(true,|id|
            new.exchange_order_id.as_ref()==Some(id))&&
        (!old.cumulative_truth_present||new.cumulative_truth_present)&&
        old.cumulative_qty_atoms<=new.cumulative_qty_atoms&&
        old.filled_qty_atoms()<=new.filled_qty_atoms()&&
        (!old.terminal||new.terminal)&&
        (!old.fee_evidence_complete||new.fee_evidence_complete)&&
        old.fills.iter().all(|(id,fill)|new.fills.get(id)==Some(fill))
    }
    pub(crate) fn fill_gate_extends(old:&PaperPartialGate,new:&PaperPartialGate)->bool{
        old.schema_version==new.schema_version&&
        old.generation==new.generation&&old.owner_id==new.owner_id&&
        old.native_spot_fee.as_ref().map_or(true,|fee|
            new.native_spot_fee.as_ref()==Some(fee))&&
        Self::leg_extends(&old.spot,&new.spot)&&
        old.shorts.len()<=new.shorts.len()&&
        old.shorts.iter().zip(new.shorts.iter()).all(|(a,b)|Self::leg_extends(a,b))&&
        old.cover.as_ref().map_or(true,|a|
            new.cover.as_ref().is_some_and(|b|Self::leg_extends(a,b)))&&
        old.sell.as_ref().map_or(true,|a|
            new.sell.as_ref().is_some_and(|b|Self::leg_extends(a,b)))
    }
    fn validate_legs(p:&PaperPortfolio,
        gates:&BTreeMap<String,PaperPartialGate>)->Result<(),OwnerError>{
        for (base,machine) in &p.positions{
            let gate=gates.get(base).ok_or(OwnerError::Invalid("unowned paper leg"))?;
            let status=gate.exposure()
                .map_err(|_|OwnerError::Invalid("invalid owned fill history"))?;
            if status.may_send_real_or_cancel{
                return Err(OwnerError::Invalid("exchange authority not allowed"))
            }
            if (gate.cover.is_some()||gate.sell.is_some())&&
               !matches!(machine.phase,PaperPhase::AwaitingSpotExit{..}){
                return Err(OwnerError::Invalid("closing mock fills outside EXIT_PENDING"))
            }
            match &machine.phase{
                PaperPhase::Hedged(pos)|PaperPhase::AwaitingSpotExit{pos,..}=>{
                    let qty=crate::fill_economics::decimal_to_atoms(
                        &format!("{:.12}",pos.spot_delivered_base))
                        .map_err(|_|OwnerError::Invalid("unrepresentable base qty"))?;
                    if status.spot_net_delivered_atoms!=qty||status.futures_filled_atoms!=qty{
                        return Err(OwnerError::Invalid("paper qty vs mock fill truth mismatch"))
                    }
                    // An UNKNOWN *closing* order must itself be persisted
                    // as an EXIT_PENDING quarantine record; this is NOT an
                    // opening hedge failure and must never unlock a retry.
                    let close_unknown=matches!(machine.phase,PaperPhase::AwaitingSpotExit{..})
                        &&!gate.restart_quarantine
                        &&gate.unknown.as_ref().is_some_and(|u|
                            gate.cover.as_ref().is_some_and(|c|
                                c.intent_id==u.intent_id&&c.client_order_id==u.client_order_id)
                            ||gate.sell.as_ref().is_some_and(|s|
                                s.intent_id==u.intent_id&&s.client_order_id==u.client_order_id));
                    if (status.unknown_or_quarantine&&!close_unknown)||
                        !status.spot_terminal||!status.spot_fee_complete||
                        !status.all_futures_terminal||
                        status.futures_filled_atoms==0||
                        status.unhedged_base_atoms!=0||
                        pos.futures_short_ns.is_none(){
                        return Err(OwnerError::Invalid("hedge unverified by mock fill ledger"))
                    }
                    if let PaperPhase::AwaitingSpotExit{
                        cover_quote,cover_ns,close_estimate_quote,..
                    }=&machine.phase{
                        // Exit PREPARATION still requires a fully owned hedge.
                        // This is not a terminal cover fill or a closed pair.
                        if !cover_quote.is_finite()||*cover_quote<=0.0||
                            !close_estimate_quote.is_finite()||
                            *cover_ns<=pos.futures_short_ns.unwrap_or(0){
                            return Err(OwnerError::Invalid("invalid pending cover estimate"))
                        }
                    }
                }
                PaperPhase::AwaitingHedge(pos)=>{
                    // Partial Binance fills remain explicitly unhedged until
                    // the final owned quantity is proven. Never promote to
                    // Hedged merely because a futures order returned an ACK.
                    if pos.futures_short_ns.is_some(){
                        return Err(OwnerError::Invalid("paper phase claims completed hedge"))
                    }
                    if status.futures_filled_atoms>0 &&
                       (!status.spot_terminal||!status.spot_fee_complete){
                        return Err(OwnerError::Invalid("futures filled without proven first-leg fee"))
                    }
                }
                PaperPhase::Flat|PaperPhase::Closed(_)=>{
                    return Err(OwnerError::Invalid("active map has terminal phase"))
                }
            }
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
 use super::*;
 use crate::native_paper_market_execution_review::{
    PaperMachine,PaperAssumptions,PaperPosition,
 };
 use crate::native_paper_partial_recovery_gate::PaperOrderReport;
 use crate::fill_economics::{decimal_to_atoms,FillEvent};
 use std::{fs,time::{SystemTime,UNIX_EPOCH}};
 fn atoms(x:&str)->u128{decimal_to_atoms(x).unwrap()}
 fn path()->PathBuf{
    let ns=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("paper-owner-review-{ns}")).join("state.jsonl")
 }
 fn position()->PaperPosition{
   PaperPosition{base:"ONE".into(),signal_ns:1,spot_buy_ns:1,
     signal_estimated_net_bps:90.0,spot_gross_base:4.0,
     spot_delivered_base:4.0,spot_paid_quote:10.0,
     futures_short_ns:None,futures_short_proceeds_quote:None,
     futures_short_base:None,hedge_entry_estimated_net_bps:None}
 }
 fn pending_gate()->PaperPartialGate{
    PaperPartialGate::new(7,"owner_one_1","ONEUSDT",
        "spot1","spot_cl1",atoms("4")).unwrap()
 }
 fn order(intent:&str,client:&str,order_id:&str,filled:&str,
     terminal:bool,rows:Vec<FillEvent>)->PaperOrderReport{
   PaperOrderReport{intent_id:intent.into(),client_order_id:client.into(),
     exchange_order_id:order_id.into(),cumulative_qty_atoms:atoms(filled),
     average_price_atoms:Some(atoms("2.5")),
     terminal,status:if terminal{"filled"}else{"partially_filled"}.into(),
     observed_at_ms:2001,fills:rows}
 }
 fn fill(id:&str,qty:&str)->FillEvent{
   FillEvent{fill_id:id.into(),qty_atoms:atoms(qty),
     price_atoms:atoms("2.5"),fee_cost_quote_atoms:Some(0),
     fee_currency:Some("USDT".into()),trade_time_ms:Some(2000)}
 }
 fn add_spot(p:&mut PaperPortfolio,g:&mut BTreeMap<String,PaperPartialGate>){
   p.stats.entries=1;
   p.positions.insert("ONE".into(),PaperMachine{
     config:PaperAssumptions::default(),
     phase:PaperPhase::AwaitingHedge(position())});
   g.insert("ONE".into(),pending_gate());
 }
 #[test] fn opening_requires_durable_owner_and_restart_is_quarantined(){
    let file=path();let mut owner=PaperOwner::start_new(&file,1_791_786_000_000).unwrap();
    assert_eq!(owner.last_seq,1);
    assert_eq!(owner.transact(|p,g|{add_spot(p,g);Ok(())}).unwrap(),2);
    assert_eq!(owner.portfolio.stats.entries,1);
    let recovered=PaperOwner::inspect_recovery(&file).unwrap().unwrap();
    assert_eq!(recovered.sequence,2);
    assert_eq!(recovered.pending_reconciliation,vec!["ONE"]);
    assert!(recovered.state.positions["ONE"].fills.restart_quarantine);
    assert!(!recovered.may_resume_paper);
    assert!(!recovered.may_trade_real);
    assert!(matches!(PaperOwner::start_new(&file,1_791_786_000_000),
      Err(OwnerError::Quarantine)));
    let _=fs::remove_dir_all(file.parent().unwrap());
 }
 #[test]fn missing_fill_ledger_rejects_without_publishing(){
    let file=path();let mut owner=PaperOwner::start_new(&file,1_791_786_000_000).unwrap();
    assert!(owner.transact(|p,_|{p.stats.entries=1;
       p.positions.insert("ONE".into(),PaperMachine{
         config:PaperAssumptions::default(),
         phase:PaperPhase::AwaitingHedge(position())});Ok(())
    }).is_err());
    assert_eq!(owner.last_seq,1);
    assert_eq!(owner.portfolio.positions.len(),0);
    assert_eq!(PaperOwner::inspect_recovery(&file).unwrap().unwrap().sequence,1);
    let _=fs::remove_dir_all(file.parent().unwrap());
 }
 #[test]fn unverifiable_hedge_never_advances_disk_or_memory(){
    let file=path();let mut owner=PaperOwner::start_new(&file,1_791_786_000_000).unwrap();
    owner.transact(|p,g|{add_spot(p,g);Ok(())}).unwrap();
    assert!(owner.transact(|p,_|{
      let mut pos=position();pos.futures_short_ns=Some(2);
      p.positions.get_mut("ONE").unwrap().phase=PaperPhase::Hedged(pos);
      Ok(())
    }).is_err());
    assert_eq!(owner.last_seq,2);
    assert!(matches!(owner.portfolio.positions["ONE"].phase,PaperPhase::AwaitingHedge(_)));
    let _=fs::remove_dir_all(file.parent().unwrap());
 }
 #[test]fn every_mock_fill_transition_is_durable_before_next_leg(){
   let file=path();let mut owner=PaperOwner::start_new(&file,1_791_786_000_000).unwrap();
   owner.transact(|p,g|{add_spot(p,g);Ok(())}).unwrap();
   let s=order("spot1","spot_cl1","100001","4",true,vec![fill("s1","4")]);
   owner.transact(|_,g|{g.get_mut("ONE").unwrap().observe_spot(&s)
       .map_err(|_|OwnerError::Invalid("spot mock"))?;Ok(())}).unwrap();
   assert_eq!(owner.last_seq,3);
   owner.transact(|_,g|{g.get_mut("ONE").unwrap().begin_paper_short(
      "fut1","fut_cl1",atoms("4"))
      .map_err(|_|OwnerError::Invalid("short prepare mock"))?;Ok(())}).unwrap();
   assert_eq!(owner.last_seq,4);
   let f=order("fut1","fut_cl1","100002","4",true,vec![fill("h1","4")]);
   owner.transact(|p,g|{
      g.get_mut("ONE").unwrap().observe_short(&f)
        .map_err(|_|OwnerError::Invalid("short mock"))?;
      let mut pos=position();pos.futures_short_ns=Some(2);
      pos.futures_short_proceeds_quote=Some(10.2);
      pos.futures_short_base=Some(4.0);
      pos.hedge_entry_estimated_net_bps=Some(70.0);
      p.stats.futures_hedges=1;
      p.positions.get_mut("ONE").unwrap().phase=PaperPhase::Hedged(pos);
      Ok(())
   }).unwrap();
   assert_eq!(owner.last_seq,5);
   let recovered=PaperOwner::inspect_recovery(&file).unwrap().unwrap();
   assert_eq!(recovered.sequence,5);
   assert_eq!(recovered.state.positions["ONE"].phase,"HEDGED");
   assert_eq!(recovered.state.positions["ONE"].fills.exposure().unwrap().unhedged_base_atoms,0);
   assert!(!recovered.may_resume_paper);
   // Valid *preparation* of leg 3 is allowed only after both opening
   // legs are terminal, fee-complete and exactly matched in the mock ledger.
   owner.transact(|p,_|{
      let pos=match &p.positions["ONE"].phase{
         PaperPhase::Hedged(pos)=>pos.clone(),
         _=>return Err(OwnerError::Invalid("missing mock opened hedge")),
      };
      p.positions.get_mut("ONE").unwrap().phase=PaperPhase::AwaitingSpotExit{
         pos,cover_quote:9.8,cover_ns:3,close_estimate_quote:0.1,
      };
      Ok(())
   }).unwrap();
   assert_eq!(owner.last_seq,6);
   let pending=PaperOwner::inspect_recovery(&file).unwrap().unwrap();
   assert_eq!(pending.state.positions["ONE"].phase,"EXIT_PENDING");
   assert_eq!(pending.state.closed,0);
   assert!(!pending.may_resume_paper);
   let _=fs::remove_dir_all(file.parent().unwrap());
 }
 #[test]fn unknown_order_blocks_next_paper_stage(){
   let file=path();let mut owner=PaperOwner::start_new(&file,1_791_786_000_000).unwrap();
   owner.transact(|p,g|{add_spot(p,g);Ok(())}).unwrap();
   owner.transact(|_,g|{
      g.get_mut("ONE").unwrap().mark_unknown("spot1","spot_cl1","NO_ACK")
         .map_err(|_|OwnerError::Invalid("unknown"))?;
      Ok(())
   }).unwrap();
   assert!(owner.transact(|p,_|{
       let mut pos=position();pos.futures_short_ns=Some(2);
       p.positions.get_mut("ONE").unwrap().phase=PaperPhase::Hedged(pos);
       Ok(())
   }).is_err());
   assert_eq!(owner.last_seq,3);
   assert!(owner.gate_by_base["ONE"].unknown.is_some());
   assert!(PaperOwner::inspect_recovery(&file).unwrap().unwrap()
      .state.positions["ONE"].fills.unknown.is_some());
   let _=fs::remove_dir_all(file.parent().unwrap());
 }
 #[test]fn corrupted_storage_freezes_owner_on_next_attempt(){
   let file=path();let mut owner=PaperOwner::start_new(&file,1_791_786_000_000).unwrap();
   fs::write(&file,b"INVALID").unwrap();
   assert!(owner.transact(|_,_|Ok(())).is_err());
   assert!(owner.frozen);
   assert!(matches!(owner.transact(|_,_|Ok(())),Err(OwnerError::Quarantine)));
   let _=fs::remove_dir_all(file.parent().unwrap());
 }
 #[test]fn premature_exit_pending_rejects_unhedged_opening(){
   let file=path();let mut owner=PaperOwner::start_new(&file,1_791_786_000_000).unwrap();
   owner.transact(|p,g|{add_spot(p,g);Ok(())}).unwrap();
   assert!(owner.transact(|p,_|{
      p.positions.get_mut("ONE").unwrap().phase=PaperPhase::AwaitingSpotExit{
         pos:position(),cover_quote:8.0,cover_ns:3,close_estimate_quote:0.1
      };Ok(())
   }).is_err());
   assert_eq!(owner.last_seq,2);
   let rec=PaperOwner::inspect_recovery(&file).unwrap().unwrap();
   assert_eq!(rec.state.positions["ONE"].phase,"AWAITING_HEDGE");
   assert!(!rec.may_resume_paper);
 }
 #[test]fn terminal_counter_without_closing_fill_proof_is_denied(){
   let file=path();
   let mut owner=PaperOwner::start_new(&file,1_791_786_000_000).unwrap();
   owner.transact(|p,g|{add_spot(p,g);Ok(())}).unwrap();
   assert!(owner.transact(|p,_|{p.stats.closed=1;Ok(())}).is_err());
   assert_eq!(owner.last_seq,2);
   assert_eq!(owner.portfolio.stats.closed,0);
   assert!(owner.portfolio.positions.contains_key("ONE"));
   assert_eq!(PaperOwner::inspect_recovery(&file).unwrap().unwrap().sequence,2);
 }
 #[test]fn r74_existing_fill_prefix_must_not_vanish_at_higher_cas_sequence(){
   let file=path();
   let mut owner=PaperOwner::start_new(&file,1_791_786_000_000).unwrap();
   owner.transact(|p,g|{add_spot(p,g);Ok(())}).unwrap();
   owner.transact(|_,g|{
     g.get_mut("ONE").unwrap().observe_spot(
       &order("spot1","spot_cl1","100001","4",true,vec![fill("s1","4")]))
       .map_err(|_|OwnerError::Invalid("spot"))?;
     Ok(())
   }).unwrap();
   let before=owner.last_seq;
   let original=owner.gate_by_base["ONE"].spot.clone();
   assert_eq!(original.filled_qty_atoms(),atoms("4"));
   // Regression: a caller with mutable paper state MUST NOT be able to
   // replace durable original fills with an older empty-but-valid ledger.
   assert!(owner.transact(|_,g|{
     *g.get_mut("ONE").unwrap()=pending_gate();
     Ok(())
   }).is_err(),"CAS revision increment must never erase previously accepted fills");
   assert_eq!(owner.last_seq,before);
   assert_eq!(owner.gate_by_base["ONE"].spot,original);
   let recovered=PaperOwner::inspect_recovery(&file).unwrap().unwrap();
   assert_eq!(recovered.sequence,before);
   assert_eq!(recovered.state.positions["ONE"].fills.spot,original);
   let _=fs::remove_dir_all(file.parent().unwrap());
 }
 #[test]fn attempted_over_limit_transition_never_commits(){
   let file=path();let mut owner=PaperOwner::start_new(&file,1_791_786_000_000).unwrap();
   assert!(owner.transact(|p,_|{p.stats.entries=501;Ok(())}).is_err());
   assert_eq!(owner.last_seq,1);
   assert_eq!(owner.portfolio.stats.entries,0);
   let _=fs::remove_dir_all(file.parent().unwrap());
 }
}
#[cfg(test)]
mod native_fee_owner_tests{
 use super::*;
 use crate::native_paper_market_execution_review::{PaperMachine,PaperAssumptions,PaperPosition};
 use crate::native_paper_partial_recovery_gate::PaperOrderReport;
 use crate::native_trade2f_asset_fee::SpotFeeFill;
 use crate::fill_economics::{decimal_to_atoms,FillEvent};
 use std::{fs,time::{SystemTime,UNIX_EPOCH}};
 fn n(x:&str)->u128{decimal_to_atoms(x).unwrap()}
 fn path()->PathBuf{
   let ns=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
   std::env::temp_dir().join(format!("paper-nativefee-{ns}")).join("journal.jsonl")
 }
 fn pos()->PaperPosition{
   PaperPosition{base:"ONE".into(),signal_ns:1,spot_buy_ns:1,
    signal_estimated_net_bps:50.,spot_gross_base:10.,
    spot_delivered_base:9.,spot_paid_quote:10.,
    futures_short_ns:None,futures_short_proceeds_quote:None,
    futures_short_base:None,hedge_entry_estimated_net_bps:None}
 }
 fn fill(id:&str,qty:&str,base:bool)->FillEvent{
   FillEvent{fill_id:id.into(),qty_atoms:n(qty),price_atoms:n("1"),
    fee_cost_quote_atoms:if base{None}else{Some(0)},
    fee_currency:Some(if base{"ONE"}else{"USDT"}.into()),
    trade_time_ms:Some(1234)}
 }
 fn report(intent:&str,client:&str,order_id:&str,qty:&str,terminal:bool,
    state:&str,fills:Vec<FillEvent>)->PaperOrderReport{
   PaperOrderReport{intent_id:intent.into(),client_order_id:client.into(),
    exchange_order_id:order_id.into(),cumulative_qty_atoms:n(qty),
    average_price_atoms:Some(n("1")),terminal,status:state.into(),
    observed_at_ms:1234,fills}
 }
 fn source()->SpotFeeFill{
   SpotFeeFill{order_id:"12345".into(),trade_id:"s1".into(),
      at_ms:1234,side:"buy".into(),inst_id:"ONE-USDT".into(),
      price_quote_e12:n("1"),gross_base_qty_e12:n("10"),
      fee_ccy:"ONE".into(),signed_fee_e12:-(n("1") as i128)}
 }
 fn session()->PaperOwner{
   let file=path();let mut o=PaperOwner::start_new(&file,1_791_786_000_000).unwrap();
   o.transact(|p,g|{
     p.stats.entries=1;
     p.positions.insert("ONE".into(),PaperMachine{
       config:PaperAssumptions::default(),phase:PaperPhase::AwaitingHedge(pos())});
     let mut gate=PaperPartialGate::new(3,"owner_ONE","ONEUSDT",
       "spot","spotcl",n("10")).unwrap();
     gate.observe_spot(&report("spot","spotcl","12345","10",true,
       "filled",vec![fill("s1","10",true)]))
       .map_err(|_|OwnerError::Invalid("spot mock fill"))?;
     g.insert("ONE".into(),gate);Ok(())
   }).unwrap();
   o
 }
 #[test]fn native_fee_fsynced_then_partial_short_reopens_as_exposed(){
   let mut o=session();assert_eq!(o.last_seq,2);
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().record_native_spot_fee(vec![source()])
       .map_err(|_|OwnerError::Invalid("native fee"))?;Ok(())
   }).unwrap();
   assert_eq!(o.last_seq,3);
   assert_eq!(o.gate_by_base["ONE"].exposure().unwrap().spot_net_delivered_atoms,n("9"));
   assert!(o.transact(|_,g|{
      g.get_mut("ONE").unwrap().begin_paper_short("bad","badc",n("10"))
         .map_err(|_|OwnerError::Invalid("overhedge"))?;Ok(())
   }).is_err());
   assert_eq!(o.last_seq,3);
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().begin_paper_short("short1","shortcl1",n("9"))
       .map_err(|_|OwnerError::Invalid("prepare short"))?;Ok(())
   }).unwrap();
   assert_eq!(o.last_seq,4);
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().observe_short(
       &report("short1","shortcl1","short_order1","5",false,
         "partially_filled",vec![fill("h1","5",false)]))
       .map_err(|_|OwnerError::Invalid("partial short"))?;Ok(())
   }).unwrap();
   assert_eq!(o.last_seq,5);
   let recovered=PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap();
   let fee=&recovered.state.positions["ONE"].fills;
   assert_eq!(fee.exposure().unwrap().spot_base_fee_atoms,n("1"));
   assert_eq!(fee.exposure().unwrap().futures_filled_atoms,n("5"));
   assert_eq!(fee.exposure().unwrap().unhedged_base_atoms,n("4"));
   assert!(fee.restart_quarantine);
   assert!(!recovered.may_resume_paper);
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().observe_short(
       &report("short1","shortcl1","short_order1","8",true,
         "canceled",vec![fill("h2","3",false)]))
       .map_err(|_|OwnerError::Invalid("terminal partial"))?;Ok(())
   }).unwrap();
   assert_eq!(o.gate_by_base["ONE"].exposure().unwrap().unhedged_base_atoms,n("1"));
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().begin_paper_short("short2","shortcl2",n("1"))
       .map_err(|_|OwnerError::Invalid("followup short"))?;Ok(())
   }).unwrap();
   o.transact(|p,g|{
     g.get_mut("ONE").unwrap().observe_short(
       &report("short2","shortcl2","short_order2","1",true,
         "filled",vec![fill("h3","1",false)]))
       .map_err(|_|OwnerError::Invalid("final short"))?;
     let mut pos=pos();pos.futures_short_ns=Some(2);
     pos.futures_short_base=Some(9.0);
     pos.futures_short_proceeds_quote=Some(9.0);
     pos.hedge_entry_estimated_net_bps=Some(50.0);
     p.stats.futures_hedges=1;
     p.positions.get_mut("ONE").unwrap().phase=PaperPhase::Hedged(pos);
     Ok(())
   }).unwrap();
   assert_eq!(o.last_seq,8);
   let rec=PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap();
   let fill=&rec.state.positions["ONE"].fills;
   assert_eq!(fill.exposure().unwrap().spot_filled_atoms,n("10"));
   assert_eq!(fill.exposure().unwrap().spot_net_delivered_atoms,n("9"));
   assert_eq!(fill.exposure().unwrap().futures_filled_atoms,n("9"));
   assert_eq!(fill.exposure().unwrap().unhedged_base_atoms,0);
   assert_eq!(rec.state.positions["ONE"].phase,"HEDGED");
   assert!(fill.restart_quarantine);
   assert!(!rec.may_resume_paper);
   assert!(!rec.may_trade_real);
   let _=fs::remove_dir_all(o.journal.parent().unwrap());
 }
 // R72: owner with durable native BASE fee and an in-flight partial short.
 fn r72_partial_fee_owner()->PaperOwner{
   let mut o=session();
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().record_native_spot_fee(vec![source()])
       .map_err(|_|OwnerError::Invalid("R72 base fee"))?;Ok(())
   }).unwrap();
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().begin_paper_short("short1","shortcl1",n("9"))
       .map_err(|_|OwnerError::Invalid("R72 intent"))?;Ok(())
   }).unwrap();
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().observe_short(&report(
       "short1","shortcl1","short_order1","5",false,
       "partially_filled",vec![fill("h1","5",false)]))
       .map_err(|_|OwnerError::Invalid("R72 first partial"))?;Ok(())
   }).unwrap();
   assert_eq!(o.last_seq,5);
   assert_eq!(o.gate_by_base["ONE"].exposure().unwrap().unhedged_base_atoms,n("4"));
   o
 }
 #[test]fn r78_public_journal_cas_must_not_accept_regressed_original_partial_fill(){
   let o=r72_partial_fee_owner();
   let sequence=o.last_seq;
   let mut candidate=ReviewSnapshot::from_paper(&o.portfolio,&o.gate_by_base).unwrap();
   let before=candidate.positions["ONE"].fills.exposure().unwrap();
   assert_eq!(before.spot_base_fee_atoms,n("1"));
   assert_eq!(before.futures_filled_atoms,n("5"));
   // Call the public durable writer directly, bypassing PaperOwner::transact.
   // A new valid snapshot with the old partial SHORT removed must DENY.
   candidate.positions.get_mut("ONE").unwrap().fills.shorts.clear();
   assert_eq!(candidate.positions["ONE"].fills.exposure().unwrap().futures_filled_atoms,0);
   let result=append_snapshot_if_prev(&o.journal,&candidate,sequence);
   assert!(result.is_err(),"R78: journal must itself enforce persisted fill ancestry");
   let recovered=PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap();
   assert_eq!(recovered.sequence,sequence);
   assert_eq!(recovered.state.positions["ONE"].fills.exposure().unwrap().futures_filled_atoms,n("5"));
   assert!(!recovered.may_resume_paper&&!recovered.may_trade_real);
   let _=fs::remove_dir_all(o.journal.parent().unwrap());
 }
 #[test]fn r78_durable_fee_partial_owner_cannot_disappear_from_cas_snapshot(){
   let mut o=r72_partial_fee_owner();
   let sequence=o.last_seq;
   let old_fee=o.gate_by_base["ONE"].native_spot_fee.clone();
   for remove in ["position_only","gate_only","both"]{
     let result=o.transact(|p,g|{
       if remove!="gate_only"{p.positions.remove("ONE");}
       if remove!="position_only"{g.remove("ONE");}
       Ok(())
     });
     assert!(result.is_err(),"R78 {remove}: durable owner may not vanish");
     assert_eq!(o.last_seq,sequence,"R78 {remove}: sequence unchanged");
     assert!(o.portfolio.positions.contains_key("ONE"));
     assert_eq!(o.gate_by_base["ONE"].native_spot_fee,old_fee);
     let disk=PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap();
     assert_eq!(disk.sequence,sequence);
     assert_eq!(disk.state.positions["ONE"].fills.exposure().unwrap().futures_filled_atoms,n("5"));
     assert!(!disk.may_resume_paper&&!disk.may_trade_real);
   }
   let _=fs::remove_dir_all(o.journal.parent().unwrap());
 }
 #[test]fn r74_fee_bearing_partial_short_history_cannot_be_dropped_by_new_cas(){
   let mut o=r72_partial_fee_owner();
   let seq=o.last_seq;
   let original=o.gate_by_base["ONE"].shorts.clone();
   assert_eq!(o.gate_by_base["ONE"].exposure().unwrap().spot_base_fee_atoms,n("1"));
   assert_eq!(o.gate_by_base["ONE"].exposure().unwrap().futures_filled_atoms,n("5"));
   assert!(o.transact(|_,g|{
     g.get_mut("ONE").unwrap().shorts.clear();
     Ok(())
   }).is_err(),"previously durable partial futures fill may not disappear");
   assert_eq!(o.last_seq,seq);
   assert_eq!(o.gate_by_base["ONE"].shorts,original);
   let disk=PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap();
   assert_eq!(disk.sequence,seq);
   assert_eq!(disk.state.positions["ONE"].fills.shorts,original);
   assert!(!disk.may_trade_real&&!disk.may_resume_paper);
   let _=fs::remove_dir_all(o.journal.parent().unwrap());
 }
 /// Invoked ONLY by the parent test, in an isolated child process.
 #[test]fn r72_partial_fee_write_fault_child(){
   if std::env::var("SPOTARB_R72_CHILD").ok().as_deref()!=Some("offline_unit_test"){return}
   let path=PathBuf::from(std::env::var("SPOTARB_R72_JOURNAL_PATH").unwrap());
   let source=std::env::var("SPOTARB_R72_CANDIDATE_PATH").unwrap();
   let state:ReviewSnapshot=serde_json::from_slice(&fs::read(source).unwrap()).unwrap();
   let r=append_snapshot_if_prev(&path,&state,5);
   assert!(r.is_err(),"write fault must never acknowledge a durable commit");
 }
 #[test]fn r72_torn_or_unacknowledged_native_fee_partial_short_fails_closed(){
   use std::process::{Command,Stdio};
   for kind in ["partial_write","after_file_sync_error"]{
     let mut o=r72_partial_fee_owner();
     let mut revised=o.gate_by_base.clone();
     revised.get_mut("ONE").unwrap().observe_short(&report(
       "short1","shortcl1","short_order1","8",false,
       "partially_filled",vec![fill("h2","3",false)]))
       .unwrap();
     let candidate=ReviewSnapshot::from_paper(&o.portfolio,&revised).unwrap();
     let file=o.journal.with_extension("r72_candidate.json");
     fs::write(&file,serde_json::to_vec(&candidate).unwrap()).unwrap();
     let child=Command::new(std::env::current_exe().unwrap())
       .arg("--exact")
       .arg("native_paper_owner_transaction_review::native_fee_owner_tests::r72_partial_fee_write_fault_child")
       .arg("--nocapture")
       .env("SPOTARB_R72_CHILD","offline_unit_test")
       .env("SPOTARB_R72_JOURNAL_PATH",&o.journal)
       .env("SPOTARB_R72_CANDIDATE_PATH",&file)
       .env("SPOTARB_TEST_JOURNAL_FAULT_CHILD","isolated_subprocess")
       .env("SPOTARB_TEST_JOURNAL_FAULT_KIND",kind)
       .stdout(Stdio::null()).stderr(Stdio::null())
       .status().unwrap();
     assert!(child.success(),"isolated child must reject {kind} without ACK");
     // Owner RAM still reflects the old, formally acknowledged 5-base short.
     assert_eq!(o.last_seq,5);
     assert_eq!(o.gate_by_base["ONE"].exposure().unwrap().futures_filled_atoms,n("5"));
     assert_eq!(o.gate_by_base["ONE"].exposure().unwrap().spot_base_fee_atoms,n("1"));
     if kind=="partial_write"{
       assert!(PaperOwner::inspect_recovery(&o.journal).is_err(),
         "torn journal must not quietly restore an older partial position");
       assert!(o.transact(|_,_|Ok(())).is_err());
       assert!(o.frozen,"ambiguous append must freeze the in-memory owner");
     }else{
       let rec=PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap();
       assert_eq!(rec.sequence,6);
       let e=rec.state.positions["ONE"].fills.exposure().unwrap();
       assert_eq!(e.spot_base_fee_atoms,n("1"));
       assert_eq!(e.futures_filled_atoms,n("8"));
       assert_eq!(e.unhedged_base_atoms,n("1"));
       assert!(e.unknown_or_quarantine);
       assert!(!rec.may_resume_paper&&!rec.may_trade_real);
       assert!(o.transact(|_,_|Ok(())).is_err(),
          "no duplicate/ACK possible using stale CAS sequence 5");
       assert!(o.frozen);
       assert_eq!(PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap().sequence,6);
     }
     assert_eq!(o.last_seq,5);
     let _=fs::remove_dir_all(o.journal.parent().unwrap());
   }
 }
 #[test]fn r72_valid_prefix_rollback_needs_independent_tip_floor(){
   let mut o=r72_partial_fee_owner();
   let old_bytes=fs::read(&o.journal).unwrap();
   assert_eq!(o.transact(|_,g|{
     g.get_mut("ONE").unwrap().observe_short(&report(
       "short1","shortcl1","short_order1","8",false,
       "partially_filled",vec![fill("h2","3",false)]))
       .map_err(|_|OwnerError::Invalid("R72 second partial"))?;Ok(())
   }).unwrap(),6);
   let tip=PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap();
   assert_eq!(tip.state.positions["ONE"].fills.exposure().unwrap().unhedged_base_atoms,n("1"));
   // Deliberate OFFLINE local rollback to a hash-correct prior prefix.
   // Local SHA verification alone cannot establish external currentness.
   fs::write(&o.journal,old_bytes).unwrap();
   let locally_valid=PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap();
   assert_eq!(locally_valid.sequence,5);
   assert_eq!(locally_valid.state.positions["ONE"].fills.exposure().unwrap().unhedged_base_atoms,n("4"));
   assert!(crate::native_paper_durable_journal_review::
      inspect_journal_against_supplied_floor(&o.journal,tip.sequence,&tip.hash).is_err(),
      "externally supplied authenticated revision floor detects an old, valid prefix");
   assert!(o.transact(|_,_|Ok(())).is_err(),
      "owner still at acknowledged CAS=6 must refuse to append onto rolled-back 5");
   assert!(o.frozen);
   assert!(!locally_valid.may_resume_paper&&!locally_valid.may_trade_real);
   let _=fs::remove_dir_all(o.journal.parent().unwrap());
 }
 fn ready_to_close()->PaperOwner{
   let mut o=session();
   o.transact(|_,g|{
      g.get_mut("ONE").unwrap().record_native_spot_fee(vec![source()])
         .map_err(|_|OwnerError::Invalid("base fee"))?;Ok(())
   }).unwrap();
   o.transact(|_,g|{
      g.get_mut("ONE").unwrap().begin_paper_short("short1","sclient",n("9"))
         .map_err(|_|OwnerError::Invalid("short intent"))?;Ok(())
   }).unwrap();
   o.transact(|p,g|{
      g.get_mut("ONE").unwrap().observe_short(&report(
         "short1","sclient","future1","9",true,"filled",
         vec![fill("hf1","9",false)]))
         .map_err(|_|OwnerError::Invalid("short fill"))?;
      let mut a=pos();a.futures_short_ns=Some(2);a.futures_short_base=Some(9.0);
      a.futures_short_proceeds_quote=Some(9.0);
      p.positions.get_mut("ONE").unwrap().phase=PaperPhase::Hedged(a);
      p.stats.futures_hedges=1;Ok(())
   }).unwrap();
   o.transact(|p,_|{
      let a=match &p.positions["ONE"].phase{
         PaperPhase::Hedged(a)=>a.clone(),_=>return Err(OwnerError::Quarantine)
      };
      p.positions.get_mut("ONE").unwrap().phase=PaperPhase::AwaitingSpotExit{
         pos:a,cover_quote:9.0,cover_ns:3,close_estimate_quote:0.1,
      };Ok(())
   }).unwrap();
   o
 }
 #[test]fn four_leg_mock_close_fills_are_cas_durable_without_fake_closed(){
   let mut o=ready_to_close();
   let starting=o.last_seq;
   assert_eq!(o.gate_by_base["ONE"].exposure().unwrap().spot_net_delivered_atoms,n("9"));
   o.transact(|_,g|{
      g.get_mut("ONE").unwrap().begin_paper_cover("cover1","cclient",n("9"))
         .map_err(|_|OwnerError::Invalid("cover intent"))?;Ok(())
   }).unwrap();
   o.transact(|_,g|{
      g.get_mut("ONE").unwrap().observe_paper_cover(&report(
        "cover1","cclient","coverord1","4",false,"partially_filled",
        vec![fill("cv1","4",false)]))
        .map_err(|_|OwnerError::Invalid("partial cover"))?;Ok(())
   }).unwrap();
   let e=o.gate_by_base["ONE"].paper_close_exposure().unwrap();
   assert_eq!(e.remaining_short_atoms,n("5"));
   assert_eq!(e.remaining_spot_base_atoms,n("9"));
   assert!(!e.mock_both_closing_legs_complete);
   assert!(o.transact(|_,g|{
     g.get_mut("ONE").unwrap().begin_paper_sell("sell1","sellcl",n("9"))
       .map_err(|_|OwnerError::Invalid("sell before complete cover"))?;Ok(())
   }).is_err());
   o.transact(|_,g|{
      g.get_mut("ONE").unwrap().observe_paper_cover(&report(
         "cover1","cclient","coverord1","9",true,"filled",
         vec![fill("cv2","5",false)]))
         .map_err(|_|OwnerError::Invalid("full cover"))?;Ok(())
   }).unwrap();
   o.transact(|_,g|{
      g.get_mut("ONE").unwrap().begin_paper_sell("sell1","sellcl",n("9"))
         .map_err(|_|OwnerError::Invalid("spot sell intent"))?;Ok(())
   }).unwrap();
   o.transact(|_,g|{
      g.get_mut("ONE").unwrap().observe_paper_sell(&report(
        "sell1","sellcl","sellord1","9",true,"filled",
        vec![fill("ss1","9",false)]))
        .map_err(|_|OwnerError::Invalid("spot sell mock fill"))?;Ok(())
   }).unwrap();
   assert_eq!(o.last_seq,starting+5);
   assert!(o.gate_by_base["ONE"].paper_close_exposure().unwrap()
      .mock_both_closing_legs_complete);
   let reopened=PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap();
   let e=reopened.state.positions["ONE"].fills.paper_close_exposure().unwrap();
   assert_eq!(e.cover_filled_atoms,n("9"));
   assert_eq!(e.spot_sold_atoms,n("9"));
   assert_eq!(e.remaining_short_atoms,0);
   assert_eq!(e.remaining_spot_base_atoms,0);
   // Recovery always quarantines even a complete local four-leg mock tape.
   assert!(!e.mock_both_closing_legs_complete);
   assert!(!e.independently_verified_closed);
   assert!(!e.real_execution_authorized);
   assert_eq!(reopened.state.closed,0);
   assert_eq!(reopened.state.positions["ONE"].phase,"EXIT_PENDING");
   assert!(!reopened.may_resume_paper);
   assert!(!reopened.may_trade_real);
 }
 #[test]fn r79_direct_journal_cannot_forge_v2_cross_source_marker(){
   let mut o=ready_to_close();
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().begin_paper_cover("c1","cc1",n("9")).unwrap();
     Ok(())
   }).unwrap();
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().observe_paper_cover(&report(
       "c1","cc1","co1","9",true,"filled",vec![fill("cf1","9",false)])).unwrap();
     Ok(())
   }).unwrap();
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().begin_paper_sell("s1","sc1",n("9")).unwrap();
     Ok(())
   }).unwrap();
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().observe_paper_sell(&report(
       "s1","sc1","so1","9",true,"filled",vec![fill("sf1","9",false)])).unwrap();
     Ok(())
   }).unwrap();
   let seq=o.last_seq;
   let mut candidate=ReviewSnapshot::from_paper(&o.portfolio,&o.gate_by_base).unwrap();
   assert!(candidate.positions["ONE"].fills.paper_close_exposure().unwrap()
     .mock_both_closing_legs_complete);
   candidate.positions.get_mut("ONE").unwrap().fills.v2_local_alignment=
     Some(crate::native_paper_v2_closure_bridge_review::PaperV2LocalAlignment{
       original_generation:o.gate_by_base["ONE"].generation,
       paper_parent_seq:seq,v2_source_revision:4,
       source_digest_sha256:"a".repeat(64),
       independent_exchange_truth:false,verified_closed:false,may_send_or_retry:false,
     });
   assert!(append_snapshot_if_prev(&o.journal,&candidate,seq).is_err(),
       "direct durable writer must not mint V2 source approval marker");
   assert_eq!(PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap().sequence,seq);
   let _=fs::remove_dir_all(o.journal.parent().unwrap());
 }
 #[test]fn missing_cover_ack_and_restart_block_sell_and_status(){
   let mut o=ready_to_close();
   o.transact(|_,g|{
      g.get_mut("ONE").unwrap().begin_paper_cover("cover1","cclient",n("9"))
         .map_err(|_|OwnerError::Invalid("cover intent"))?;Ok(())
   }).unwrap();
   o.transact(|_,g|{
      g.get_mut("ONE").unwrap().mark_unknown("cover1","cclient","timeout")
         .map_err(|_|OwnerError::Invalid("unknown cover"))?;Ok(())
   }).unwrap();
   assert!(o.transact(|_,g|{
     g.get_mut("ONE").unwrap().begin_paper_sell("sell","sc",n("9"))
         .map_err(|_|OwnerError::Quarantine)?;Ok(())
   }).is_err());
   let reopened=PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap();
   assert!(reopened.state.positions["ONE"].fills.unknown.is_some());
   assert!(reopened.state.positions["ONE"].fills.restart_quarantine);
   assert!(!reopened.may_resume_paper);
 }
 #[test]fn partial_spot_sell_cannot_prove_flat_after_restart(){
   let mut o=ready_to_close();
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().begin_paper_cover("c1","cc1",n("9"))
       .map_err(|_|OwnerError::Invalid("cover prepare"))?;Ok(())
   }).unwrap();
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().observe_paper_cover(&report(
       "c1","cc1","c_order","9",true,"filled",vec![fill("cfill","9",false)]))
       .map_err(|_|OwnerError::Invalid("cover proof"))?;Ok(())
   }).unwrap();
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().begin_paper_sell("s1","sc1",n("9"))
       .map_err(|_|OwnerError::Invalid("sell prepare"))?;Ok(())
   }).unwrap();
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().observe_paper_sell(&report(
       "s1","sc1","s_order","3",false,"partially_filled",
       vec![fill("sfill","3",false)]))
       .map_err(|_|OwnerError::Invalid("partial sell"))?;Ok(())
   }).unwrap();
   let live=o.gate_by_base["ONE"].paper_close_exposure().unwrap();
   assert_eq!(live.remaining_short_atoms,0);
   assert_eq!(live.remaining_spot_base_atoms,n("6"));
   assert!(!live.mock_both_closing_legs_complete);
   let r=PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap();
   let recovered=r.state.positions["ONE"].fills.paper_close_exposure().unwrap();
   assert_eq!(recovered.remaining_spot_base_atoms,n("6"));
   assert!(!r.may_resume_paper);
   assert_eq!(r.state.closed,0);
 }
 #[test]fn wrong_close_identity_is_denied_without_advancing_cas(){
   let mut o=ready_to_close();
   o.transact(|_,g|{
     g.get_mut("ONE").unwrap().begin_paper_cover("c1","cc1",n("9"))
       .map_err(|_|OwnerError::Invalid("cover prepare"))?;Ok(())
   }).unwrap();
   let before=o.last_seq;
   assert!(o.transact(|_,g|{
     g.get_mut("ONE").unwrap().observe_paper_cover(&report(
       "foreign","cc1","unknown","9",true,"filled",
       vec![fill("f1","9",false)]))
       .map_err(|_|OwnerError::Invalid("mismatched cover"))?;Ok(())
   }).is_err());
   assert_eq!(o.last_seq,before);
   assert_eq!(o.gate_by_base["ONE"].paper_close_exposure().unwrap()
      .cover_filled_atoms,0);
   assert_eq!(PaperOwner::inspect_recovery(&o.journal).unwrap().unwrap().sequence,before);
 }
}

#[cfg(test)]
mod paper_cas_race_tests{
 use super::*;
 use std::{fs,thread,time::{SystemTime,UNIX_EPOCH},sync::{Arc,Barrier}};
 fn file(tag:&str)->PathBuf{
   let ns=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
   std::env::temp_dir().join(format!("spotarb-cas-{tag}-{ns}")).join("journal.jsonl")
 }
 fn clone_writer(w:&PaperOwner)->PaperOwner{
   PaperOwner{portfolio:w.portfolio.clone(),gate_by_base:w.gate_by_base.clone(),
     journal:w.journal.clone(),last_seq:w.last_seq,frozen:false}
 }
 fn entry(w:&mut PaperOwner)->Result<u64,OwnerError>{
   w.transact(|p,_|{p.stats.entries=1;Ok(())})
 }
 #[test]fn stale_owner_is_frozen_without_publish(){
   let path=file("stale");
   let mut winner=PaperOwner::start_new(&path,1_791_786_000_000).unwrap();
   let mut stale=clone_writer(&winner);
   assert_eq!(entry(&mut winner).unwrap(),2);
   assert!(matches!(entry(&mut stale),Err(OwnerError::Journal(_))));
   assert!(stale.frozen);
   assert_eq!(stale.last_seq,1);
   assert_eq!(stale.portfolio.stats.entries,0);
   assert_eq!(PaperOwner::inspect_recovery(&path).unwrap().unwrap().sequence,2);
   assert_eq!(PaperOwner::inspect_recovery(&path).unwrap().unwrap().state.entries,1);
   let _=fs::remove_dir_all(path.parent().unwrap());
 }
 #[test]fn two_threads_same_parent_exactly_one_append(){
   let path=file("two");
   let first=PaperOwner::start_new(&path,1_791_786_000_000).unwrap();
   let second=clone_writer(&first);
   let barrier=Arc::new(Barrier::new(2));
   let b=barrier.clone();
   let h1=thread::spawn(move||{let mut owner=first;b.wait();let r=entry(&mut owner);(r,owner)});
   let b=barrier.clone();
   let h2=thread::spawn(move||{let mut owner=second;b.wait();let r=entry(&mut owner);(r,owner)});
   let (r1,o1)=h1.join().unwrap();
   let (r2,o2)=h2.join().unwrap();
   assert_eq!(u8::from(r1.is_ok())+u8::from(r2.is_ok()),1);
   let denied=if r1.is_err(){o1}else{o2};
   assert!(denied.frozen);
   assert_eq!(denied.last_seq,1);
   assert_eq!(denied.portfolio.stats.entries,0);
   let recovered=PaperOwner::inspect_recovery(&path).unwrap().unwrap();
   assert_eq!(recovered.sequence,2);
   assert_eq!(recovered.state.entries,1);
   assert!(!recovered.may_resume_paper);
   assert!(!recovered.may_trade_real);
   let _=fs::remove_dir_all(path.parent().unwrap());
 }
 #[test]fn disk_commit_without_memory_publication_stale_writer_denied(){
    let path=file("crashwindow");
    let mut owner=PaperOwner::start_new(&path,1_791_786_000_000).unwrap();
    let mut after=owner.portfolio.clone();
    after.stats.entries=1;
    let state=ReviewSnapshot::from_paper(&after,&owner.gate_by_base).unwrap();
    assert_eq!(append_snapshot_if_prev(&path,&state,1).unwrap(),2);
    // A crash between fsync and in-memory publish must NOT permit replay.
    assert!(matches!(entry(&mut owner),Err(OwnerError::Journal(_))));
    assert!(owner.frozen);
    assert_eq!(owner.portfolio.stats.entries,0);
    let seen=PaperOwner::inspect_recovery(&path).unwrap().unwrap();
    assert_eq!(seen.sequence,2);
    assert_eq!(seen.state.entries,1);
    assert!(!seen.may_resume_paper);
    let _=fs::remove_dir_all(path.parent().unwrap());
 }
 #[test]fn writer_lock_contention_freezes_fail_closed(){
    let path=file("lock");
    let mut owner=PaperOwner::start_new(&path,1_791_786_000_000).unwrap();
    let lock=path.with_extension("writer_lock");
    fs::write(&lock,b"other writer").unwrap();
    assert!(matches!(entry(&mut owner),Err(OwnerError::Journal(_))));
    assert!(owner.frozen);
    assert_eq!(owner.last_seq,1);
    assert_eq!(owner.portfolio.stats.entries,0);
    fs::remove_file(lock).unwrap();
    assert_eq!(PaperOwner::inspect_recovery(&path).unwrap().unwrap().sequence,1);
    let _=fs::remove_dir_all(path.parent().unwrap());
 }
 #[test]fn malformed_journal_freezes_even_before_candidate_publish(){
    let path=file("damaged");
    let mut owner=PaperOwner::start_new(&path,1_791_786_000_000).unwrap();
    fs::write(&path,b"partial").unwrap();
    assert!(matches!(entry(&mut owner),Err(OwnerError::Journal(_))));
    assert!(owner.frozen);
    assert_eq!(owner.portfolio.stats.entries,0);
    assert!(PaperOwner::inspect_recovery(&path).is_err());
    let _=fs::remove_dir_all(path.parent().unwrap());
 }
}
