//! R32D6C8B2B2B3: origin-of-entry anchor, NOT native-close authority.
//!
//! Origin is captured WITH the first durable configured Prepared entry. It
//! binds the plan, policy SHA, original spot intent and client ID. It cannot
//! prove subsequent opening fills or the still absent closing-order lineage.
//! Neither this struct nor matching caller IDs can authorize trading.
use crate::{
    authoritative_state::{
        NativeEntryAuthority,NativeEntryPlan,NativePositionBundle,NativeStateError,
        PositionProvenance,
    },
    native_trading_config::NativeEntryPolicyBinding,
};
use openssl::sha::sha256;
use serde::{Serialize,Deserialize};

fn hex(bytes:&[u8])->String{
    const H:&[u8;16]=b"0123456789abcdef";
    let mut s=String::with_capacity(bytes.len()*2);
    for &b in bytes{s.push(H[(b>>4) as usize]as char);s.push(H[(b&15)as usize]as char)}
    s
}
fn hash<T:Serialize>(t:&T)->Result<String,NativeStateError>{
    let bytes=serde_json::to_vec(t).map_err(|_|NativeStateError::Json)?;
    Ok(hex(&sha256(&bytes)))
}
fn digest_ok(h:&str)->bool{
    h.len()==64 && h.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase())
}
fn id_ok(id:&str)->bool{
    !id.is_empty()&&id.len()<=96 &&
    id.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'-'||b==b'_'||b==b'.')
}
/// Verified against the *persisted entry plan and initial spot intent*.
/// This is a policy/intent ancestry key, not a universal exchange position ID.
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeEntryOriginAnchor {
    entry_first_generation:u64,
    position_key_sha256:String,
    plan_sha256:String,
    policy_config_sha256:String,
    policy_binding_sha256:String,
    spot_open_intent_id:String,
    spot_open_client_id:String,
    opened_at_ms:u64,
    initial_qty_e18:u128,
    safety_generation:u64,
    original_plan:NativeEntryPlan,
    original_policy:NativeEntryPolicyBinding,
}
impl NativeEntryOriginAnchor {
    pub fn position_key(&self)->&str{&self.position_key_sha256}
    pub fn policy_sha256(&self)->&str{&self.policy_config_sha256}
    pub fn initial_generation(&self)->u64{self.entry_first_generation}
    pub fn spot_client_id(&self)->&str{&self.spot_open_client_id}
    pub fn may_send_order(&self)->bool{false}
    pub fn may_verify_closed(&self)->bool{false}
    pub fn opening_two_legs_proven(&self)->bool{false}
    pub fn original_close_lineage_proven(&self)->bool{false}
    fn calculate_position_key(
        entry_first_generation:u64,plan_hash:&str,binding_hash:&str,
        policy_hash:&str,intent:&str,client:&str,
    )->Result<String,NativeStateError>{
        hash(&("R32D6C8B2B2B3_ENTRY_ORIGIN_V1",entry_first_generation,
            plan_hash,binding_hash,policy_hash,intent,client))
    }
    pub fn seal_configured_prepared(
        generation:u64,plan:&NativeEntryPlan,binding:&NativeEntryPolicyBinding,
        intent:&str,client:&str,
    )->Result<Self,NativeStateError>{
        if generation<2||plan.created_at_ms==0||plan.planned_qty_atoms_e18==0
            ||!id_ok(intent)||!id_ok(client)||!digest_ok(&binding.config_sha256){
            return Err(NativeStateError::Invalid("origin input identity invalid"))
        }
        binding.validate_in_state(plan).map_err(|_|NativeStateError::Invalid(
            "origin policy did not pass bound quote/safety validation"))?;
        let plan_sha256=hash(plan)?;
        let policy_binding_sha256=hash(binding)?;
        let position_key_sha256=Self::calculate_position_key(
            generation,&plan_sha256,&policy_binding_sha256,
            &binding.config_sha256,intent,client)?;
        Ok(Self{
            entry_first_generation:generation,position_key_sha256,
            plan_sha256,policy_config_sha256:binding.config_sha256.clone(),
            policy_binding_sha256,spot_open_intent_id:intent.into(),
            spot_open_client_id:client.into(),opened_at_ms:plan.created_at_ms,
            initial_qty_e18:plan.planned_qty_atoms_e18,
            safety_generation:plan.safety_generation,
            original_plan:plan.clone(),original_policy:binding.clone(),
        })
    }
    pub fn verify_entry(
        &self,entry:&NativeEntryAuthority,current_generation:u64,
    )->Result<(),NativeStateError>{
        let b=entry.policy_binding.as_ref().ok_or(NativeStateError::Invalid(
            "origin lost original config binding"))?;
        let expected=Self::seal_configured_prepared(self.entry_first_generation,
            &entry.plan,b,&self.spot_open_intent_id,&self.spot_open_client_id)?;
        if expected!=*self ||self.original_plan!=entry.plan
            ||self.original_policy!=*b|| current_generation<self.entry_first_generation
            ||self.safety_generation!=entry.plan.safety_generation
            ||self.opened_at_ms!=entry.plan.created_at_ms
            ||self.initial_qty_e18!=entry.plan.planned_qty_atoms_e18{
            return Err(NativeStateError::Invalid("origin seal mismatch"))
        }
        if !entry.intents.iter().any(|i|i.id==self.spot_open_intent_id
            &&i.client_order_id.as_deref()==Some(self.spot_open_client_id.as_str())){
            return Err(NativeStateError::Invalid("original opening intent missing"))
        }
        let spot=entry.economics.entry.spot.order(&self.spot_open_intent_id)
            .map_err(|_|NativeStateError::Invalid("original opening spot order absent"))?;
        if spot.client_order_id!=self.spot_open_client_id
            ||spot.intent_id!=self.spot_open_intent_id
            ||spot.symbol!=entry.plan.spot_symbol
            ||spot.venue!="okx"||spot.side!="buy"
            ||spot.requested_qty_atoms.checked_mul(1_000_000)!=Some(self.initial_qty_e18){
            return Err(NativeStateError::Invalid("original opening order altered"))
        }
        Ok(())
    }
    /// Carries a frozen *opening-plan* anchor through a future valid Native
    /// position transition. This does not certify a real fill/hedge, and the
    /// R32D6C4 guard still denies Native active in ordinary linked code.
    pub fn verify_active_plan(
        &self,position:&NativePositionBundle,
    )->Result<(),NativeStateError>{
        let core=&position.core;
        let recomputed=Self::seal_configured_prepared(self.entry_first_generation,
            &self.original_plan,&self.original_policy,
            &self.spot_open_intent_id,&self.spot_open_client_id)?;
        if recomputed!=*self
            ||core.base!=self.original_plan.base
            ||core.spot_symbol!=self.original_plan.spot_symbol
            ||core.futures_symbol!=self.original_plan.futures_symbol
            ||core.spot_baseline_atoms_e18!=self.original_plan.spot_baseline_atoms_e18
            ||core.order_quote_atoms_e18!=self.original_plan.order_quote_atoms_e18 {
            return Err(NativeStateError::Invalid("native active opening plan changed"))
        }
        if position.provenance!=PositionProvenance::Native
            ||position.reconciliation_required
            ||!digest_ok(&self.position_key_sha256)
            ||!digest_ok(&self.policy_config_sha256)
            ||self.entry_first_generation<2
            ||self.initial_qty_e18==0
            ||self.opened_at_ms!=core.opened_at_ms
            ||core.spot_net_atoms_e18>self.initial_qty_e18
            ||core.futures_target_atoms_e18>self.initial_qty_e18
            ||self.safety_generation!=position.lifecycle.safety_generation
            ||position.intents.iter().all(|i|i.id!=self.spot_open_intent_id
                ||i.client_order_id.as_deref()!=Some(self.spot_open_client_id.as_str())){
            return Err(NativeStateError::Invalid("native active origin plan missing/mismatch"))
        }
        let economics=position.economics.as_ref().ok_or(
            NativeStateError::Invalid("native active original economics absent"))?;
        let open=economics.entry.spot.order(&self.spot_open_intent_id)
            .map_err(|_|NativeStateError::Invalid("native active original spot missing"))?;
        if open.client_order_id!=self.spot_open_client_id
            ||open.requested_qty_atoms.checked_mul(1_000_000)!=Some(self.initial_qty_e18){
            return Err(NativeStateError::Invalid("native active original client/quantity mismatch"))
        }
        Ok(())
    }
}


/// Read a previously CAS+fsync-persisted *configured opening intent*.
/// This is not a query for close authority; a historical entry anchor with
/// zero exchange fills is perfectly valid and MUST NOT certify inventory.
pub fn read_persisted_origin(
    store:&crate::authoritative_state::NativeStateStore,
    expected_generation:u64,
)->Result<NativeEntryOriginAnchor,NativeStateError>{
    let state=store.load()?.ok_or(NativeStateError::Generation)?;
    if state.generation!=expected_generation{
        return Err(NativeStateError::Generation)
    }
    if state.active.is_some(){
        return Err(NativeStateError::Invalid("origin reader only supports unpromoted entry"))
    }
    let entry=state.entry.as_ref().ok_or(
        NativeStateError::Invalid("persisted prepared entry absent"))?;
    let origin=entry.origin.as_ref().ok_or(
        NativeStateError::Invalid("entry started without configured immutable origin"))?;
    origin.verify_entry(entry,state.generation)?;
    Ok(origin.clone())
}
