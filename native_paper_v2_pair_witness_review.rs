//! Astra P0-03 / P1-07 pair-tip witness protocol (zero-network).
//! Ed25519 authenticates ONE response, not external key provenance,
//! independently durable monotonicity or freshness of remote latest state.
//! No exchange access, accounts, order authority or automatic recovery.
use openssl::{pkey::{Id,PKey},sign::Verifier};
use serde::{Serialize,Deserialize};

pub const PAIR_REPLY_DOMAIN:&str="SPOTARB/V2-PAPER/PAIR-LATEST/ED25519/V1";
fn valid_hex(v:&str,n:usize)->bool{
    v.len()==n*2&&v.bytes().all(|c|c.is_ascii_hexdigit()&&!c.is_ascii_uppercase())
}
fn ident(v:&str)->bool{
    !v.is_empty()&&v.len()<=128&&v.bytes()
        .all(|c|c.is_ascii_alphanumeric()||c==b'_'||c==b'-'||c==b'.')
}
fn unhex(v:&str,n:usize)->Result<Vec<u8>,PairWitnessError>{
    if !valid_hex(v,n){return Err(PairWitnessError::Malformed)}
    let mut bytes=Vec::with_capacity(n);
    for pair in v.as_bytes().chunks_exact(2){
        let h=(pair[0] as char).to_digit(16).ok_or(PairWitnessError::Malformed)?;
        let l=(pair[1] as char).to_digit(16).ok_or(PairWitnessError::Malformed)?;
        bytes.push((h*16+l) as u8);
    }
    Ok(bytes)
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairTip{
    pub owner_generation:u64,
    pub immutable_genesis_sha256:String,
    pub v2_revision:u64,
    pub v2_entry_sha256:String,
    pub paper_sequence:u64,
    pub paper_entry_sha256:String,
}
impl PairTip{
    pub fn validate(&self)->Result<(),PairWitnessError>{
        if self.owner_generation==0||self.v2_revision==0||self.paper_sequence==0||
            !valid_hex(&self.immutable_genesis_sha256,32)||
            !valid_hex(&self.v2_entry_sha256,32)||
            !valid_hex(&self.paper_entry_sha256,32){
            return Err(PairWitnessError::Malformed)
        }
        Ok(())
    }
    /// Caller MUST independently validate the COMPLETE local V2 hash chain,
    /// V2 owner, PAPER journal chain and generation/position ancestry.
    /// This constructor only retains an already-reviewed pair of tips.
    pub fn from_validated_local_tips(
        v2:&crate::native_trade2f3_v2_revision_journal::V2JournalEntry,
        paper:&crate::native_paper_durable_journal_review::RecoveredPaper,
    )->Result<Self,PairWitnessError>{
        if paper.state.positions.is_empty()||
            paper.state.positions.values().any(|row|
                row.fills.generation!=v2.original_owner_generation||
                !row.quarantine||!row.fills.restart_quarantine){
            return Err(PairWitnessError::LocalMismatch)
        }
        let p=Self{
            owner_generation:v2.original_owner_generation,
            immutable_genesis_sha256:v2.immutable_genesis_sha256.clone(),
            v2_revision:v2.revision,v2_entry_sha256:v2.entry_sha256.clone(),
            paper_sequence:paper.sequence,paper_entry_sha256:paper.hash.clone(),
        };
        p.validate()?;Ok(p)
    }
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairSignedLatestPayload{
    pub protocol:u32,
    pub domain:String,
    pub witness_id:String,
    pub request_nonce_hex:String,
    /// Witness-monotonic pair CAS generation, not any on-host revision.
    pub witness_pair_revision:u64,
    pub latest:PairTip,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairSignedLatest{
    pub payload:PairSignedLatestPayload,
    pub signature_ed25519_hex:String,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct PairPinnedChallenge{
    pub trusted_witness_id:String,
    pub trusted_public_key_hex:String,
    pub freshly_requested_nonce_hex:String,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum PairWitnessError{
    Malformed,
    MissingIndependentPin,
    WrongWitnessOrNonce,
    InvalidSignature,
    LocalMismatch,
    LocalBehindWitness,
    WitnessBehindLocal,
    InconsistentPairRevision,
    Unavailable,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct PairWitnessDiagnostic{
    pub signed_response_valid:bool,
    pub pair_revision:u64,
    pub same_local_pair:bool,
    /// These MUST remain false in this zero-network review.
    pub external_durability_proven:bool,
    pub remote_currentness_proven:bool,
    pub may_resume_or_trade:bool,
    pub independently_verified_closed:bool,
}
pub fn pair_latest_message(p:&PairSignedLatestPayload)
    ->Result<Vec<u8>,PairWitnessError>{
    if p.protocol!=1||p.domain!=PAIR_REPLY_DOMAIN||
        !ident(&p.witness_id)||!valid_hex(&p.request_nonce_hex,32)||
        p.witness_pair_revision==0{
        return Err(PairWitnessError::Malformed)
    }
    p.latest.validate()?;
    serde_json::to_vec(p).map_err(|_|PairWitnessError::Malformed)
}
pub fn verify_signed_pair_latest(
    response:Option<&PairSignedLatest>,
    pinned:&PairPinnedChallenge,
    local:&PairTip,
)->Result<PairWitnessDiagnostic,PairWitnessError>{
    let r=response.ok_or(PairWitnessError::Unavailable)?;
    if pinned.trusted_public_key_hex.is_empty(){
        return Err(PairWitnessError::MissingIndependentPin)
    }
    if pinned.trusted_witness_id!=r.payload.witness_id||
       pinned.freshly_requested_nonce_hex!=r.payload.request_nonce_hex{
       return Err(PairWitnessError::WrongWitnessOrNonce)
    }
    local.validate()?;
    let key=unhex(&pinned.trusted_public_key_hex,32)?;
    let sign=unhex(&r.signature_ed25519_hex,64)?;
    let key=PKey::public_key_from_raw_bytes(&key,Id::ED25519)
       .map_err(|_|PairWitnessError::Malformed)?;
    let mut verifier=Verifier::new_without_digest(&key)
       .map_err(|_|PairWitnessError::Malformed)?;
    if !verifier.verify_oneshot(&sign,&pair_latest_message(&r.payload)?)
       .map_err(|_|PairWitnessError::Malformed)?{
       return Err(PairWitnessError::InvalidSignature)
    }
    let current=&r.payload.latest;
    if local.owner_generation!=current.owner_generation||
       local.immutable_genesis_sha256!=current.immutable_genesis_sha256{
       return Err(PairWitnessError::LocalMismatch)
    }
    if local.v2_revision<current.v2_revision||
       local.paper_sequence<current.paper_sequence{
       return Err(PairWitnessError::LocalBehindWitness)
    }
    if local.v2_revision>current.v2_revision||
       local.paper_sequence>current.paper_sequence{
       return Err(PairWitnessError::WitnessBehindLocal)
    }
    if local.v2_entry_sha256!=current.v2_entry_sha256||
       local.paper_entry_sha256!=current.paper_entry_sha256{
       return Err(PairWitnessError::LocalMismatch)
    }
    Ok(PairWitnessDiagnostic{
       signed_response_valid:true,pair_revision:r.payload.witness_pair_revision,
       same_local_pair:true,external_durability_proven:false,
       remote_currentness_proven:false,may_resume_or_trade:false,
       independently_verified_closed:false,
    })
}
/// Caller-supplied previously observed tip. If persisted on this same VM,
/// it is NOT an independent or antirollback-anchored high-watermark.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct PairObservedFloor{
    pub witness_id:String,
    pub witness_pair_revision:u64,
    pub latest:PairTip,
}
/// Verify fresh Ed25519 challenge first, then compare with a previously
/// observed signed witness state. No replays, forks at the same revision,
/// gaps or retrograde source histories. Remains ZERO-NETWORK diagnostic:
/// the caller MUST NOT use this as a trading/recovery authorization.
pub fn verify_signed_pair_latest_after_observed_floor(
    response:Option<&PairSignedLatest>,
    pinned:&PairPinnedChallenge,
    local:&PairTip,
    prior:Option<&PairObservedFloor>,
)->Result<PairWitnessDiagnostic,PairWitnessError>{
    let diagnostic=verify_signed_pair_latest(response,pinned,local)?;
    let Some(previous)=prior else{return Ok(diagnostic)};
    let signed=response.ok_or(PairWitnessError::Unavailable)?;
    previous.latest.validate()?;
    if !ident(&previous.witness_id)||previous.witness_pair_revision==0||
        previous.witness_id!=pinned.trusted_witness_id{
        return Err(PairWitnessError::MissingIndependentPin)
    }
    let current=signed.payload.witness_pair_revision;
    if current<previous.witness_pair_revision||
       current>previous.witness_pair_revision.saturating_add(1){
        return Err(PairWitnessError::InconsistentPairRevision)
    }
    let old=&previous.latest;
    let new=&signed.payload.latest;
    if old.owner_generation!=new.owner_generation||
       old.immutable_genesis_sha256!=new.immutable_genesis_sha256{
        return Err(PairWitnessError::LocalMismatch)
    }
    if current==previous.witness_pair_revision{
        if new!=old{return Err(PairWitnessError::LocalMismatch)}
    }else{
        if new==old||
           new.v2_revision<old.v2_revision||
           new.paper_sequence<old.paper_sequence||
           new.v2_revision-old.v2_revision>1||
           new.paper_sequence-old.paper_sequence>1{
            return Err(PairWitnessError::InconsistentPairRevision)
        }
        if (new.v2_revision==old.v2_revision&&
            new.v2_entry_sha256!=old.v2_entry_sha256)||
           (new.paper_sequence==old.paper_sequence&&
            new.paper_entry_sha256!=old.paper_entry_sha256){
            return Err(PairWitnessError::LocalMismatch)
        }
    }
    Ok(diagnostic)
}

/// A strictly in-process simulator for protocol state-machine tests.
/// NOT independent, durable, authenticated remote hardware or deployment.
#[cfg(test)]
mod tests{
    use super::*;
    use std::sync::Mutex;
    use openssl::{pkey::PKey,sign::Signer};
    fn h(n:char)->String{std::iter::repeat(n).take(64).collect()}
    fn tip(v2:u64,paper:u64)->PairTip{
        PairTip{owner_generation:99,immutable_genesis_sha256:h('a'),
            v2_revision:v2,v2_entry_sha256:h(if v2==4{'b'}else{'c'}),
            paper_sequence:paper,paper_entry_sha256:h(if paper==2{'d'}else{'e'})}
    }
    struct LocalMockPairWitness{state:Mutex<Option<(u64,PairTip)>>}
    impl LocalMockPairWitness{
        fn new()->Self{Self{state:Mutex::new(None)}}
        fn cas(&self,expected:u64,next:PairTip)->Result<u64,PairWitnessError>{
            next.validate()?;
            let mut g=self.state.lock().map_err(|_|PairWitnessError::Unavailable)?;
            let prior=g.clone();
            let old_rev=prior.as_ref().map_or(0,|x|x.0);
            if old_rev!=expected{return Err(PairWitnessError::InconsistentPairRevision)}
            if let Some((_,old))=prior{
                if old.owner_generation!=next.owner_generation||
                   old.immutable_genesis_sha256!=next.immutable_genesis_sha256{
                    return Err(PairWitnessError::LocalMismatch)
                }
                // Never silently skip more than one rev from either
                // journal; after a prolonged outage the witness needs a
                // separate, explicitly audited catch-up/recovery protocol.
                if next.v2_revision<old.v2_revision||
                   next.paper_sequence<old.paper_sequence||
                   next.v2_revision-old.v2_revision>1||
                   next.paper_sequence-old.paper_sequence>1||
                   next==old{
                    return Err(PairWitnessError::InconsistentPairRevision)
                }
                if next.v2_revision==old.v2_revision&&
                   next.v2_entry_sha256!=old.v2_entry_sha256||
                   next.paper_sequence==old.paper_sequence&&
                   next.paper_entry_sha256!=old.paper_entry_sha256{
                    return Err(PairWitnessError::LocalMismatch)
                }
            }
            let seq=old_rev.checked_add(1)
                .ok_or(PairWitnessError::InconsistentPairRevision)?;
            *g=Some((seq,next));
            Ok(seq)
        }
        fn read(&self,nonce:&str,key:&PKey<openssl::pkey::Private>)
            ->Result<PairSignedLatest,PairWitnessError>{
            let g=self.state.lock().map_err(|_|PairWitnessError::Unavailable)?;
            let (revision,latest)=g.as_ref().ok_or(PairWitnessError::Unavailable)?;
            let payload=PairSignedLatestPayload{
                protocol:1,domain:PAIR_REPLY_DOMAIN.into(),
                witness_id:"simulated_witness".into(),
                request_nonce_hex:nonce.to_owned(),
                witness_pair_revision:*revision,latest:latest.clone(),
            };
            let mut signer=Signer::new_without_digest(key)
                .map_err(|_|PairWitnessError::Malformed)?;
            let signature=signer.sign_oneshot_to_vec(&pair_latest_message(&payload)?)
                .map_err(|_|PairWitnessError::Malformed)?;
            Ok(PairSignedLatest{
                payload,signature_ed25519_hex:signature.iter()
                   .map(|b|format!("{b:02x}")).collect(),
            })
        }
    }
    #[test]
    fn signed_pair_observed_floor_denies_replay_fork_and_revision_gap(){
        let key=PKey::generate_ed25519().unwrap();
        let pin=PairPinnedChallenge{
            trusted_witness_id:"simulated_witness".into(),
            trusted_public_key_hex:key.raw_public_key().unwrap().iter()
                .map(|v|format!("{v:02x}")).collect(),
            freshly_requested_nonce_hex:h('1'),
        };
        let mock=LocalMockPairWitness::new();
        mock.cas(0,tip(4,2)).unwrap();
        mock.cas(1,tip(4,3)).unwrap();
        let floor=PairObservedFloor{
            witness_id:"simulated_witness".into(),
            witness_pair_revision:2,
            latest:tip(4,3),
        };
        let signed=mock.read(&pin.freshly_requested_nonce_hex,&key).unwrap();
        let result=verify_signed_pair_latest_after_observed_floor(
            Some(&signed),&pin,&tip(4,3),Some(&floor)).unwrap();
        assert!(result.signed_response_valid);
        assert!(!result.remote_currentness_proven&&!result.may_resume_or_trade);
        *mock.state.lock().unwrap()=Some((1,tip(4,3)));
        let replay=mock.read(&pin.freshly_requested_nonce_hex,&key).unwrap();
        assert!(verify_signed_pair_latest(Some(&replay),&pin,&tip(4,3)).is_ok());
        assert_eq!(verify_signed_pair_latest_after_observed_floor(
            Some(&replay),&pin,&tip(4,3),Some(&floor)),
            Err(PairWitnessError::InconsistentPairRevision));
        *mock.state.lock().unwrap()=Some((2,tip(4,2)));
        let fork=mock.read(&pin.freshly_requested_nonce_hex,&key).unwrap();
        assert_eq!(verify_signed_pair_latest_after_observed_floor(
            Some(&fork),&pin,&tip(4,2),Some(&floor)),
            Err(PairWitnessError::LocalMismatch));
        *mock.state.lock().unwrap()=Some((3,tip(5,4)));
        let next=mock.read(&pin.freshly_requested_nonce_hex,&key).unwrap();
        assert!(verify_signed_pair_latest_after_observed_floor(
            Some(&next),&pin,&tip(5,4),Some(&floor)).is_ok());
        *mock.state.lock().unwrap()=Some((5,tip(5,4)));
        let gap=mock.read(&pin.freshly_requested_nonce_hex,&key).unwrap();
        assert_eq!(verify_signed_pair_latest_after_observed_floor(
            Some(&gap),&pin,&tip(5,4),Some(&floor)),
            Err(PairWitnessError::InconsistentPairRevision));
        let mut wrong_nonce=pin.clone();
        wrong_nonce.freshly_requested_nonce_hex=h('2');
        assert_eq!(verify_signed_pair_latest_after_observed_floor(
            Some(&next),&wrong_nonce,&tip(5,4),Some(&floor)),
            Err(PairWitnessError::WrongWitnessOrNonce));
    }
    #[test]fn one_signed_pair_tip_fresh_nonce_and_fail_closed_permissions(){
        let key=PKey::generate_ed25519().unwrap();
        let pin=PairPinnedChallenge{
           trusted_witness_id:"simulated_witness".into(),
           trusted_public_key_hex:key.raw_public_key().unwrap().iter()
                .map(|b|format!("{b:02x}")).collect(),
           freshly_requested_nonce_hex:h('1'),
        };
        let mock=LocalMockPairWitness::new();
        assert_eq!(mock.cas(0,tip(4,2)).unwrap(),1);
        assert!(mock.cas(0,tip(4,3)).is_err());
        assert_eq!(mock.cas(1,tip(4,3)).unwrap(),2);
        assert!(mock.cas(2,tip(4,3)).is_err());
        assert!(mock.cas(2,tip(3,4)).is_err());
        let signed=mock.read(&pin.freshly_requested_nonce_hex,&key).unwrap();
        let good=verify_signed_pair_latest(Some(&signed),&pin,&tip(4,3)).unwrap();
        assert_eq!(good.pair_revision,2);
        assert!(good.signed_response_valid&&good.same_local_pair);
        assert!(!good.external_durability_proven&&!good.remote_currentness_proven);
        assert!(!good.may_resume_or_trade&&!good.independently_verified_closed);
        assert_eq!(verify_signed_pair_latest(Some(&signed),&pin,&tip(4,2)),
            Err(PairWitnessError::LocalBehindWitness));
        assert_eq!(verify_signed_pair_latest(Some(&signed),&pin,&tip(5,3)),
            Err(PairWitnessError::WitnessBehindLocal));
        assert_eq!(verify_signed_pair_latest(None,&pin,&tip(4,3)),
            Err(PairWitnessError::Unavailable));
        let mut other=pin.clone();other.freshly_requested_nonce_hex=h('2');
        assert_eq!(verify_signed_pair_latest(Some(&signed),&other,&tip(4,3)),
            Err(PairWitnessError::WrongWitnessOrNonce));
        other=pin.clone();other.trusted_public_key_hex.clear();
        assert_eq!(verify_signed_pair_latest(Some(&signed),&other,&tip(4,3)),
            Err(PairWitnessError::MissingIndependentPin));
        let mut forged=signed.clone();forged.payload.latest.paper_entry_sha256=h('0');
        assert_eq!(verify_signed_pair_latest(Some(&forged),&pin,&tip(4,3)),
            Err(PairWitnessError::InvalidSignature));
    }
    #[test]fn concurrent_mock_cas_exactly_one_commits_and_gap_deny(){
        use std::sync::{Arc,Barrier};
        use std::thread;
        let mock=Arc::new(LocalMockPairWitness::new());
        assert_eq!(mock.cas(0,tip(4,2)).unwrap(),1);
        let barrier=Arc::new(Barrier::new(9));
        let mut workers=Vec::new();
        for _ in 0..8{
            let m=mock.clone();
            let b=barrier.clone();
            workers.push(thread::spawn(move||{
                b.wait();m.cas(1,tip(4,3))
            }));
        }
        barrier.wait();
        let responses=workers.into_iter().map(|w|w.join().unwrap())
           .collect::<Vec<_>>();
        assert_eq!(responses.iter().filter(|r|r.is_ok()).count(),1);
        assert_eq!(mock.state.lock().unwrap().as_ref().unwrap().0,2);
        assert_eq!(mock.cas(2,tip(4,5)),
           Err(PairWitnessError::InconsistentPairRevision));
        assert_eq!(mock.cas(2,tip(6,3)),
           Err(PairWitnessError::InconsistentPairRevision));
        assert_eq!(mock.cas(2,tip(5,4)).unwrap(),3);
        assert_eq!(mock.cas(3,tip(5,4)),
           Err(PairWitnessError::InconsistentPairRevision));
    }
    #[test]fn wrong_signer_and_foreign_identity_fail_closed(){
        let proper=PKey::generate_ed25519().unwrap();
        let foreign=PKey::generate_ed25519().unwrap();
        let mock=LocalMockPairWitness::new();
        mock.cas(0,tip(4,2)).unwrap();
        let nonce=h('a');
        let pin=PairPinnedChallenge{
            trusted_witness_id:"simulated_witness".into(),
            trusted_public_key_hex:proper.raw_public_key().unwrap().iter()
                .map(|b|format!("{b:02x}")).collect(),
            freshly_requested_nonce_hex:nonce.clone(),
        };
        let issued_by_wrong_key=mock.read(&nonce,&foreign).unwrap();
        assert_eq!(verify_signed_pair_latest(
            Some(&issued_by_wrong_key),&pin,&tip(4,2)),
            Err(PairWitnessError::InvalidSignature));
        let mut foreign_id=mock.read(&nonce,&proper).unwrap();
        foreign_id.payload.witness_id="other_witness".into();
        assert_eq!(verify_signed_pair_latest(
            Some(&foreign_id),&pin,&tip(4,2)),
            Err(PairWitnessError::WrongWitnessOrNonce));
        let accepted=mock.read(&nonce,&proper).unwrap();
        let mut stale=pin.clone();stale.freshly_requested_nonce_hex=h('b');
        assert_eq!(verify_signed_pair_latest(Some(&accepted),&stale,&tip(4,2)),
            Err(PairWitnessError::WrongWitnessOrNonce));
    }
    #[test]fn pair_hash_mismatch_and_fork_fail_closed(){
        let key=PKey::generate_ed25519().unwrap();
        let mock=LocalMockPairWitness::new();
        mock.cas(0,tip(4,2)).unwrap();
        let pin=PairPinnedChallenge{
           trusted_witness_id:"simulated_witness".into(),
           trusted_public_key_hex:key.raw_public_key().unwrap().iter()
                .map(|b|format!("{b:02x}")).collect(),
           freshly_requested_nonce_hex:h('9'),
        };
        let signed=mock.read(&pin.freshly_requested_nonce_hex,&key).unwrap();
        let mut local=tip(4,2);local.paper_entry_sha256=h('f');
        assert_eq!(verify_signed_pair_latest(Some(&signed),&pin,&local),
            Err(PairWitnessError::LocalMismatch));
        let mut fork=tip(4,2);fork.v2_entry_sha256=h('f');
        assert_eq!(mock.cas(1,fork),Err(PairWitnessError::LocalMismatch));
        let mut next=tip(5,2);next.paper_entry_sha256=h('e');
        assert_eq!(mock.cas(1,next),Err(PairWitnessError::LocalMismatch));
    }
}
