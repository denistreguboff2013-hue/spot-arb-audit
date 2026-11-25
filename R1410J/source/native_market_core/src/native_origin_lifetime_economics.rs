//! R32D6C8B2B2B4C4A: NON-AUTHORIZING round-trip economics and lifetime GAP.
//! Exact four-leg quote flows/fees are recomputed from B3-B4C3 frozen data.
//! Binance reported close-only realized PNL must match opening and closing
//! short notional.  Funding observed only in last short window: this is
//! NEVER lifetime or real net PNL. Never a Native VerifiedClosed authority.
use crate::authoritative_state::{
    NativeEntryAuthority,NativeAuthoritativeState,NativeEntryPhase,NativeStateError,NativeStateStore,
};
use crate::fill_economics::LegFillLedger;
use openssl::sha::sha256;
use serde::{Serialize,Deserialize};

fn deny(s:&'static str)->NativeStateError{NativeStateError::Invalid(s)}
fn hash<T:Serialize>(v:&T)->Result<String,NativeStateError>{
    Ok(sha256(&serde_json::to_vec(v).map_err(|_|NativeStateError::Json)?)
        .iter().map(|x|format!("{x:02x}")).collect())
}
fn quoted(leg:&LegFillLedger)->Result<i128,NativeStateError>{
    if !leg.terminal||!leg.fee_evidence_complete
        ||leg.filled_qty_atoms()==0||leg.fills.is_empty()
        ||leg.fills.values().any(|f|f.fee_cost_quote_atoms.map_or(true,|x|x<0)
            ||f.fee_currency.as_deref()!=Some("USDT")
            ||f.trade_time_ms.is_none()){
        return Err(deny("four-leg fill fee or time incomplete"))
    }
    i128::try_from(leg.quote_notional_atoms().ok_or(deny("missing exact quote cashflow"))?)
        .map_err(|_|deny("quote notional overflow"))
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeShadowLifetimeEconomicsGap {
    original_position_key_sha256:String,
    original_policy_sha256:String,
    source_generation:u64,
    source_evidence_sha256:String,
    opening_at_ms:u64,
    last_bounded_account_window_at_ms:u64,
    spot_open_notional_e12:i128,
    spot_close_notional_e12:i128,
    futures_open_notional_e12:i128,
    futures_close_notional_e12:i128,
    trade_fee_total_e12:i128,
    futures_reported_close_pnl_e12:i128,
    futures_recomputed_close_pnl_e12:i128,
    gross_round_trip_quote_e12:i128,
    verified_only_close_window_funding_e12:i128,
    observed_window_net_quote_e12:i128,
    /// Unknown contributions outside the bounded close-source window are NOT
    /// set to zero, even if the observable interval has one funding event.
    full_open_to_close_funding_history_complete:bool,
    full_open_to_close_bill_history_complete:bool,
    independent_exchange_provenance_complete:bool,
    external_anti_rollback_history_proven:bool,
    gross_equals_authoritative_account_pnl:bool,
    definitive_lifetime_net_pnl_known:bool,
    may_send_order:bool,
    may_verify_closed:bool,
    may_release_cooldown:bool,
}
impl NativeShadowLifetimeEconomicsGap {
    pub fn observed_window_net_e12(&self)->i128{self.observed_window_net_quote_e12}
    pub fn recomputed_futures_close_pnl_e12(&self)->i128{self.futures_recomputed_close_pnl_e12}
    pub fn reported_futures_close_pnl_e12(&self)->i128{self.futures_reported_close_pnl_e12}
    pub fn known_trade_fees_e12(&self)->i128{self.trade_fee_total_e12}
    pub fn bounded_funding_e12(&self)->i128{self.verified_only_close_window_funding_e12}
    pub fn funding_lifetime_complete(&self)->bool{self.full_open_to_close_funding_history_complete}
    pub fn authoritative_net_pnl_known(&self)->bool{self.definitive_lifetime_net_pnl_known}
    pub fn may_verify_closed(&self)->bool{self.may_verify_closed}
    pub fn may_send_order(&self)->bool{self.may_send_order}
    pub fn may_release_cooldown(&self)->bool{self.may_release_cooldown}
    pub fn source_generation(&self)->u64{self.source_generation}
    fn calculate(e:&NativeEntryAuthority,generation:u64)->Result<Self,NativeStateError>{
        let original=e.origin.as_ref().ok_or(deny("original position origin absent"))?;
        let opening=e.origin_dual_opening_link.as_ref().ok_or(deny("two-leg opening link absent"))?;
        let bounds=e.shadow_close_account_window.as_ref()
            .ok_or(deny("bounded post-close source not captured"))?;
        let closed=e.shadow_close_receipt.as_ref().ok_or(deny("close fills absent"))?;
        let first=e.spot_prehedge_evidence.as_ref().ok_or(deny("OKX opening ledger absent"))?;
        let futures=e.recovery_checkpoint.as_ref().ok_or(deny("Binance entry ledger absent"))?;
        original.verify_entry(e,generation)?;
        opening.verify(e,generation)?;
        bounds.verify(e,generation)?;
        closed.verify(e,generation)?;
        let spot_open=&first.exact_ledger;
        let futures_open=&futures.ledger;
        let (spot_close,futures_close)=closed.bounded_economic_ledgers();
        let expected=closed.closing_qty_e12();
        if e.phase!=NativeEntryPhase::RecoveryRequired
            ||spot_open.venue!="okx"||spot_open.side!="buy"
            ||futures_open.venue!="binance"||futures_open.side!="sell"
            ||spot_close.venue!="okx"||spot_close.side!="sell"
            ||futures_close.venue!="binance"||futures_close.side!="buy"
            ||spot_open.symbol!=e.plan.spot_symbol
            ||spot_close.symbol!=e.plan.spot_symbol
            ||futures_open.symbol!=e.plan.futures_symbol
            ||futures_close.symbol!=e.plan.futures_symbol
            ||expected==0
            ||spot_open.filled_qty_atoms()!=expected
            ||spot_close.filled_qty_atoms()!=expected
            ||futures_open.filled_qty_atoms()!=expected
            ||futures_close.filled_qty_atoms()!=expected
            ||opening.exposure_remaining_e18()!=0
            ||bounds.account_reads()!=8
            ||bounds.funding_observed_e12()==i128::MIN
            ||e.plan.created_at_ms>=bounds.observation_time_ms(){
            return Err(deny("full four-leg bounded economics lineage incomplete"))
        }
        let a=quoted(spot_open)?;
        let b=quoted(spot_close)?;
        let c=quoted(futures_open)?;
        let d=quoted(futures_close)?;
        let costs=[
            spot_open.fee_cost_quote_atoms,spot_close.fee_cost_quote_atoms,
            futures_open.fee_cost_quote_atoms,futures_close.fee_cost_quote_atoms,
        ];
        let mut fee=0i128;
        for f in costs{
            if f<0{return Err(deny("unknown fee rebate or negative quote cost"))}
            fee=fee.checked_add(f).ok_or(deny("four-leg fees overflow"))?;
        }
        let futures_realized=c.checked_sub(d).ok_or(deny("futures pnl overflow"))?;
        if futures_realized!=closed.futures_reported_close_pnl_e12(){
            return Err(deny("Binance close PNL inconsistent with original short entry VWAP"))
        }
        let gross=b.checked_sub(a).and_then(|x|x.checked_add(futures_realized))
            .ok_or(deny("round trip gross overflow"))?;
        let observed_funding=bounds.funding_observed_e12();
        let net=gross.checked_sub(fee).and_then(|x|x.checked_add(observed_funding))
            .ok_or(deny("bounded cashflow overflow"))?;
        let evidence_hash=hash(&("B4C4A_FOUR_LEG_ORIGINAL_EVIDENCE_V1",
            original,opening,bounds,closed,spot_open,futures_open,
            spot_close,futures_close))?;
        Ok(Self{
            original_position_key_sha256:original.position_key().into(),
            original_policy_sha256:original.policy_sha256().into(),
            source_generation:generation,source_evidence_sha256:evidence_hash,
            opening_at_ms:e.plan.created_at_ms,
            last_bounded_account_window_at_ms:bounds.observation_time_ms(),
            spot_open_notional_e12:a,spot_close_notional_e12:b,
            futures_open_notional_e12:c,futures_close_notional_e12:d,
            trade_fee_total_e12:fee,
            futures_reported_close_pnl_e12:closed.futures_reported_close_pnl_e12(),
            futures_recomputed_close_pnl_e12:futures_realized,
            gross_round_trip_quote_e12:gross,
            verified_only_close_window_funding_e12:observed_funding,
            observed_window_net_quote_e12:net,
            full_open_to_close_funding_history_complete:false,
            full_open_to_close_bill_history_complete:false,
            independent_exchange_provenance_complete:false,
            external_anti_rollback_history_proven:false,
            gross_equals_authoritative_account_pnl:false,
            definitive_lifetime_net_pnl_known:false,
            may_send_order:false,may_verify_closed:false,may_release_cooldown:false,
        })
    }
    pub fn verify(&self,e:&NativeEntryAuthority,current_generation:u64)
        ->Result<(),NativeStateError>{
        if current_generation<=self.source_generation
            ||self.full_open_to_close_funding_history_complete
            ||self.full_open_to_close_bill_history_complete
            ||self.independent_exchange_provenance_complete
            ||self.external_anti_rollback_history_proven
            ||self.gross_equals_authoritative_account_pnl
            ||self.definitive_lifetime_net_pnl_known
            ||self.may_send_order||self.may_verify_closed||self.may_release_cooldown
            ||Self::calculate(e,self.source_generation)?!=*self{
            return Err(deny("four-leg bounded economics seal changed or overclaims authority"))
        }
        Ok(())
    }
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum ShadowLifetimeEconomicsWrite{Recorded,Idempotent}
impl NativeStateStore{
    /// Diagnostic economics-only: owner-CAS seals four original real prices
    /// in review/mock data, never sets flat, VerifiedClosed or real cooldown.
    pub fn record_shadow_lifetime_economics_gap(
        &self,expected_generation:u64,
    )->Result<(NativeAuthoritativeState,ShadowLifetimeEconomicsWrite),NativeStateError>{
        let snapshot=self.load()?.ok_or(NativeStateError::Generation)?;
        if snapshot.generation!=expected_generation{return Err(NativeStateError::Generation)}
        if let Some(entry)=snapshot.entry.as_ref(){
            if let Some(old)=entry.shadow_lifetime_economics.as_ref(){
                old.verify(entry,snapshot.generation)?;
                return Ok((snapshot,ShadowLifetimeEconomicsWrite::Idempotent))
            }
        }
        let (saved,_)=self.transact(expected_generation,|state|{
            if state.active.is_some(){return Err(deny("Native active hard blocked"))}
            let e=state.entry.as_mut().ok_or(deny("original opening absent"))?;
            if e.shadow_lifetime_economics.is_some(){
                return Err(deny("duplicate economics record denied"))
            }
            e.shadow_lifetime_economics=Some(NativeShadowLifetimeEconomicsGap::calculate(
                e,state.generation)?);
            Ok(())
        })?;
        Ok((saved,ShadowLifetimeEconomicsWrite::Recorded))
    }
}
