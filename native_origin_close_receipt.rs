//! R32D6C8B2B2B4C2: signed-GET (synthetic executor only) closing order
//! receipts, tied to durable B3/B4B/B4C1 entry/close identity in SAME
//! Native owner CAS. This never certifies flat account or global funding.
use crate::{
    authoritative_state::{
        NativeEntryAuthority,NativeAuthoritativeState,NativeEntryPhase,
        NativeStateError,NativeStateStore,
    },
    fill_economics::LegFillLedger,
    native_close_readonly::{DualCloseCandidate,DualCloseReadOnlyCandidate},
};
use openssl::sha::sha256;
use serde::{Serialize,Deserialize};

fn candidate_digest(c:&DualCloseCandidate)->Result<String,NativeStateError>{
    hash(&(&c.position_identity,&c.base,c.created_at_ms,c.expected_qty_e12,
        &c.spot.symbol,&c.spot.client_id,&c.spot.exchange_order_id,
        &c.futures.symbol,&c.futures.client_id,&c.futures.exchange_order_id))
}
fn invalid(s:&'static str)->NativeStateError{NativeStateError::Invalid(s)}
fn hash<T:Serialize>(data:&T)->Result<String,NativeStateError>{
    let bytes=serde_json::to_vec(data).map_err(|_|NativeStateError::Json)?;
    Ok(sha256(&bytes).iter().map(|x|format!("{x:02x}")).collect())
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeShadowCloseReceipt {
    original_position_key_sha256:String,
    original_policy_sha256:String,
    proposed_close_sha256:String,
    source_shadow_generation:u64,
    observed_at_ms:u64,
    spot_ledger:LegFillLedger,
    futures_ledger:LegFillLedger,
    reported_futures_close_pnl_e12:i128,
    exact_receipts_sha256:String,
    closing_qty_e12:u128,
    unmatched_spot_residual_e18:u128,
    bounded_order_reads:u8,
    bounded_fill_reads:u8,
    two_close_order_gets_matched:bool,
    exchange_global_account_history_complete:bool,
    cross_venue_atomicity_proven:bool,
    may_send_order:bool,
    may_verify_closed:bool,
    may_release_real_cooldown:bool,
}
impl NativeShadowCloseReceipt {
    pub fn position_key(&self)->&str{&self.original_position_key_sha256}
    pub fn spot_close_trade_ids(&self)->Vec<String>{self.spot_ledger.fills.keys().cloned().collect()}
    pub fn futures_close_trade_ids(&self)->Vec<String>{self.futures_ledger.fills.keys().cloned().collect()}
    pub fn closing_qty_e12(&self)->u128{self.closing_qty_e12}
    pub fn residual_spot_e18(&self)->u128{self.unmatched_spot_residual_e18}
    pub fn source_generation(&self)->u64{self.source_shadow_generation}
    pub fn two_bounded_order_reads_matched(&self)->bool{self.two_close_order_gets_matched}
    pub fn may_send_order(&self)->bool{self.may_send_order}
    pub fn may_verify_closed(&self)->bool{self.may_verify_closed}
    pub fn may_release_cooldown(&self)->bool{self.may_release_real_cooldown}
    pub fn futures_reported_close_pnl_e12(&self)->i128{self.reported_futures_close_pnl_e12}
    /// Only the opaque GET-derived original exact fills, no caller loose tuple.
    pub(crate) fn exact_receipt_matches(
        &self,receipt:&DualCloseReadOnlyCandidate
    )->bool{
        receipt.spot_ledger()==&self.spot_ledger
            &&receipt.futures_ledger()==&self.futures_ledger
            &&receipt.futures_reported_realized_pnl_e12()
                ==self.reported_futures_close_pnl_e12
            &&receipt.read_counts()==(2,4)
            &&receipt.fill_counts()==(self.spot_ledger.fills.len(),
                                      self.futures_ledger.fills.len())
            &&!receipt.close_verified()&&!receipt.may_send_order()
    }
    /// Read-only original stored OKX SELL; it is not a trade permit or flatness proof.
    pub fn observed_spot_sell_ledger(&self)->&LegFillLedger{&self.spot_ledger}
    pub(crate) fn bounded_economic_ledgers(&self)->(&LegFillLedger,&LegFillLedger){
        (&self.spot_ledger,&self.futures_ledger)
    }
    pub fn lifetime_funding_complete(&self)->bool{false}
    pub fn entire_spot_account_flat(&self)->bool{false}

    fn compare_ledgers(
        entry:&NativeEntryAuthority,
        candidate:&DualCloseCandidate,
        spot:&LegFillLedger,
        fut:&LegFillLedger,
    )->Result<(),NativeStateError>{
        let s=&candidate.spot;
        let f=&candidate.futures;
        let expected=candidate.expected_qty_e12;
        let prior_spot=entry.spot_prehedge_evidence.as_ref()
            .ok_or(invalid("previous opening OKX fills missing"))?;
        let prior_futures=entry.recovery_checkpoint.as_ref()
            .ok_or(invalid("previous Binance opening fills missing"))?;
        if spot.fills.keys().any(|id|prior_spot.exact_ledger.fills.contains_key(id))
            ||fut.fills.keys().any(|id|prior_futures.ledger.fills.contains_key(id)){
            return Err(invalid("closing trade IDs reused from original opening"))
        }
        if entry.phase!=NativeEntryPhase::RecoveryRequired
            ||spot.venue!="okx"||spot.side!="sell"
            ||fut.venue!="binance"||fut.side!="buy"
            ||spot.symbol!=s.symbol||fut.symbol!=f.symbol
            ||spot.client_order_id!=s.client_id||fut.client_order_id!=f.client_id
            ||spot.exchange_order_id.as_deref()!=Some(s.exchange_order_id.as_str())
            ||fut.exchange_order_id.as_deref()!=Some(f.exchange_order_id.as_str())
            ||spot.requested_qty_atoms!=expected||fut.requested_qty_atoms!=expected
            ||spot.filled_qty_atoms()!=expected||fut.filled_qty_atoms()!=expected
            ||!spot.terminal||!fut.terminal
            ||spot.fills.is_empty()||fut.fills.is_empty()
            ||!spot.fee_evidence_complete||!fut.fee_evidence_complete
            ||spot.fee_cost_quote_atoms<0||fut.fee_cost_quote_atoms<0
            ||spot.fills.values().chain(fut.fills.values()).any(|v|
                v.trade_time_ms.map_or(true,|t|t<candidate.created_at_ms)
                ||v.qty_atoms==0||v.price_atoms==0
                ||v.fee_cost_quote_atoms.map_or(true,|c|c<0))
            ||spot.intent_id!=candidate.position_identity
            ||fut.intent_id!=candidate.position_identity {
            return Err(invalid("read-only close fills not matching exact persisted proposal"))
        }
        Ok(())
    }
    fn make(
        e:&NativeEntryAuthority,
        source_gen:u64,
        receipt:&DualCloseReadOnlyCandidate,
        observed_at_ms:u64,
    )->Result<Self,NativeStateError>{
        let fence=e.shadow_close_identity.as_ref()
            .ok_or(invalid("original bounded close proposal missing"))?;
        fence.verify(e,source_gen)?;
        let candidate=fence.to_candidate(e);
        let opened=e.origin_dual_opening_link.as_ref()
            .ok_or(invalid("opening dual origin missing"))?;
        if source_gen<=fence.source_generation()
            ||observed_at_ms<candidate.created_at_ms
            ||observed_at_ms-candidate.created_at_ms>600_000
            ||receipt.position_identity()!=candidate.position_identity
            ||receipt.close_verified()||receipt.may_send_order()
            ||receipt.futures_flat_proven()||receipt.all_wallet_balance_brackets_proven()
            ||receipt.currency_bills_complete()||receipt.funding_complete()
            ||receipt.global_history_complete()||receipt.cross_venue_atomicity_proven()
            ||receipt.fill_counts().0==0||receipt.fill_counts().1==0
            ||receipt.read_counts()!=(2,4)
            ||fence.residual_spot_e18()!=opened.exposure_remaining_e18()
            ||fence.request_quantity_e12().checked_mul(1_000_000)
                !=Some(opened.futures_short_e18()) {
            return Err(invalid("closing GET evidence is unbound or overclaims account truth"))
        }
        Self::compare_ledgers(e,&candidate,receipt.spot_ledger(),receipt.futures_ledger())?;
        let close_hash=hash(&(
            "R32D6C8B2B2B4C2_EXACT_CLOSE_RECEIPTS_V1",
            candidate_digest(&candidate)?,receipt.spot_ledger(),receipt.futures_ledger(),
            receipt.futures_reported_realized_pnl_e12(),
            receipt.read_counts(),receipt.fill_counts(),observed_at_ms,
        ))?;
        Ok(Self{
            original_position_key_sha256:candidate.position_identity,
            original_policy_sha256:e.origin.as_ref().ok_or(invalid("origin missing"))?
                .policy_sha256().to_owned(),
            proposed_close_sha256:hash(fence)?,
            source_shadow_generation:source_gen,
            observed_at_ms,
            spot_ledger:receipt.spot_ledger().clone(),
            futures_ledger:receipt.futures_ledger().clone(),
            reported_futures_close_pnl_e12:receipt.futures_reported_realized_pnl_e12(),
            exact_receipts_sha256:close_hash,
            closing_qty_e12:candidate.expected_qty_e12,
            unmatched_spot_residual_e18:fence.residual_spot_e18(),
            bounded_order_reads:2,bounded_fill_reads:4,
            two_close_order_gets_matched:true,
            exchange_global_account_history_complete:false,
            cross_venue_atomicity_proven:false,
            may_send_order:false,may_verify_closed:false,may_release_real_cooldown:false,
        })
    }
    pub fn verify(&self,e:&NativeEntryAuthority,generation:u64)->Result<(),NativeStateError>{
        let fence=e.shadow_close_identity.as_ref().ok_or(invalid("shadow proposal missing"))?;
        let candidate=fence.to_candidate(e);
        let persisted_receipts_hash=hash(&(
            "R32D6C8B2B2B4C2_EXACT_CLOSE_RECEIPTS_V1",
            candidate_digest(&candidate)?,&self.spot_ledger,&self.futures_ledger,
            self.reported_futures_close_pnl_e12,
            (self.bounded_order_reads,self.bounded_fill_reads),
            (self.spot_ledger.fills.len(),self.futures_ledger.fills.len()),
            self.observed_at_ms,
        ))?;
        if generation<=self.source_shadow_generation
            ||self.source_shadow_generation<=fence.source_generation()
            ||self.observed_at_ms<candidate.created_at_ms
            ||self.observed_at_ms-candidate.created_at_ms>600_000
            ||self.original_position_key_sha256!=candidate.position_identity
            ||self.original_policy_sha256!=e.origin.as_ref().ok_or(invalid("origin missing"))?
                .policy_sha256()
            ||self.proposed_close_sha256!=hash(fence)?
            ||self.closing_qty_e12!=candidate.expected_qty_e12
            ||self.unmatched_spot_residual_e18!=fence.residual_spot_e18()
            ||self.bounded_order_reads!=2||self.bounded_fill_reads!=4
            ||!self.two_close_order_gets_matched
            ||self.exchange_global_account_history_complete
            ||self.cross_venue_atomicity_proven
            ||self.may_send_order||self.may_verify_closed||self.may_release_real_cooldown
            ||self.exact_receipts_sha256!=persisted_receipts_hash {
            return Err(invalid("saved close receipts lost exact original origin binding"))
        }
        Self::compare_ledgers(e,&candidate,&self.spot_ledger,&self.futures_ledger)
    }
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum ShadowCloseReceiptWrite{Recorded,Idempotent}
impl NativeStateStore {
    /// Only an already sealed opaque read-only response from the existing
    /// synthetic signed GET validator can be supplied to this owner CAS.
    /// Does NOT call exchanges, create a mutation permit, mark VerifiedClosed,
    /// release real cooldown, or move the existing RecoveryRequired exposure.
    pub fn record_shadow_close_receipts(
        &self,expected_generation:u64,receipt:&DualCloseReadOnlyCandidate,
        observed_at_ms:u64,
    )->Result<(NativeAuthoritativeState,ShadowCloseReceiptWrite),NativeStateError>{
        let snapshot=self.load()?.ok_or(NativeStateError::Generation)?;
        if snapshot.generation!=expected_generation{return Err(NativeStateError::Generation)}
        if let Some(e)=snapshot.entry.as_ref(){
            if let Some(previous)=e.shadow_close_receipt.as_ref(){
                previous.verify(e,snapshot.generation)?;
                let rebuilt=NativeShadowCloseReceipt::make(
                    e,previous.source_shadow_generation,receipt,observed_at_ms)?;
                if *previous==rebuilt{
                    return Ok((snapshot,ShadowCloseReceiptWrite::Idempotent))
                }
                return Err(invalid("conflicting exact close exchange receipt"))
            }
        }
        let (updated,_)=self.transact(expected_generation,|state|{
            if state.active.is_some(){return Err(invalid("Native active never allowed"))}
            let e=state.entry.as_mut().ok_or(invalid("original opening absent"))?;
            if e.shadow_close_receipt.is_some(){return Err(invalid("racing duplicate close receipt"))}
            e.shadow_close_receipt=Some(NativeShadowCloseReceipt::make(
                e,state.generation,receipt,observed_at_ms)?);
            Ok(())
        })?;
        Ok((updated,ShadowCloseReceiptWrite::Recorded))
    }
}
