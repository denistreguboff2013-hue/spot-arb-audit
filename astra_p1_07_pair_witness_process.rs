//! P1-07: separate OS PIDs exercise the signed pair-tip challenge/reply.
//! ZERO NETWORK, ephemeral LOCAL test-only signer key; no off-VM trust,
//! no independent durability or remote currentness, no order permission.
use openssl::{pkey::{PKey,Id},sign::Signer};
use spotarb_native_market_core::native_paper_v2_pair_witness_review::{
    PairTip,PairSignedLatest,PairSignedLatestPayload,PairObservedFloor,
    PairPinnedChallenge,PairWitnessError,PAIR_REPLY_DOMAIN,pair_latest_message,
    verify_signed_pair_latest,verify_signed_pair_latest_after_observed_floor,
};
use std::{
    env,fs::{self,File,OpenOptions},io::Write,
    os::unix::fs::{OpenOptionsExt,PermissionsExt},
    path::Path,process::{Command,Stdio},time::{SystemTime,UNIX_EPOCH},
};
fn h(c:char)->String{std::iter::repeat(c).take(64).collect()}
fn tip(v2:u64,paper:u64)->PairTip{
    PairTip{
        owner_generation:99,immutable_genesis_sha256:h('a'),
        v2_revision:v2,v2_entry_sha256:h(if v2==4{'b'}else{'c'}),
        paper_sequence:paper,paper_entry_sha256:h(if paper==3{'d'}else{'e'}),
    }
}
fn put_new(path:&Path,bytes:&[u8]){
    let mut f=OpenOptions::new().write(true).create_new(true)
        .mode(0o600).open(path).unwrap();
    f.write_all(bytes).unwrap();
    f.sync_all().unwrap();
    File::open(path.parent().unwrap()).unwrap().sync_all().unwrap();
}
fn test_root()->std::path::PathBuf{
    let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let p=env::temp_dir().join(format!("astra_pair_pid_{}_{}",std::process::id(),n));
    fs::create_dir(&p).unwrap();
    fs::set_permissions(&p,fs::Permissions::from_mode(0o700)).unwrap();
    p
}
#[test]
fn process_pair_witness_worker(){
    let Ok(role)=env::var("ASTRA_PAIR_CHILD_ROLE") else{return};
    let root=std::path::PathBuf::from(env::var("ASTRA_PAIR_CHILD_DIR").unwrap());
    if role=="sign"{
        let secret=fs::read(root.join("test_signer_key")).unwrap();
        let key=PKey::private_key_from_raw_bytes(&secret,Id::ED25519).unwrap();
        let payload=PairSignedLatestPayload{
            protocol:1,domain:PAIR_REPLY_DOMAIN.into(),
            witness_id:"local_test_witness".into(),
            request_nonce_hex:h('1'),witness_pair_revision:2,
            latest:tip(4,3),
        };
        let message=pair_latest_message(&payload).unwrap();
        let mut signer=Signer::new_without_digest(&key).unwrap();
        let sig=signer.sign_oneshot_to_vec(&message).unwrap();
        let signed=PairSignedLatest{
            payload,signature_ed25519_hex:sig.iter()
                .map(|b|format!("{b:02x}")).collect(),
        };
        put_new(&root.join("signed_reply.json"),
            &serde_json::to_vec(&signed).unwrap());
        // Intentional lost-ACK style exit after file AND directory fsync.
        std::process::exit(70);
    }
    let public=fs::read(root.join("test_pinned_public_key")).unwrap();
    let pin=PairPinnedChallenge{
        trusted_witness_id:"local_test_witness".into(),
        trusted_public_key_hex:public.iter().map(|b|format!("{b:02x}")).collect(),
        freshly_requested_nonce_hex:h('1'),
    };
    let mut signed:PairSignedLatest=serde_json::from_slice(
        &fs::read(root.join("signed_reply.json")).unwrap()).unwrap();
    let original_floor=PairObservedFloor{
        witness_id:"local_test_witness".into(),
        witness_pair_revision:2,latest:tip(4,3),
    };
    match role.as_str(){
        "verify"=>{
            let outcome=verify_signed_pair_latest_after_observed_floor(
                Some(&signed),&pin,&tip(4,3),Some(&original_floor)).unwrap();
            assert!(outcome.signed_response_valid&&outcome.same_local_pair);
            assert!(!outcome.external_durability_proven);
            assert!(!outcome.remote_currentness_proven);
            assert!(!outcome.may_resume_or_trade);
            assert!(!outcome.independently_verified_closed);
            std::process::exit(71);
        }
        "replay"=>{
            let later=PairObservedFloor{
                witness_id:"local_test_witness".into(),
                witness_pair_revision:3,latest:tip(5,4),
            };
            // A signature alone still validates its historical contents.
            assert!(verify_signed_pair_latest(
                Some(&signed),&pin,&tip(4,3)).is_ok());
            assert_eq!(verify_signed_pair_latest_after_observed_floor(
                Some(&signed),&pin,&tip(4,3),Some(&later)),
                Err(PairWitnessError::InconsistentPairRevision));
            std::process::exit(72);
        }
        "wrong_nonce"=>{
            let mut wrong=pin.clone();
            wrong.freshly_requested_nonce_hex=h('2');
            assert_eq!(verify_signed_pair_latest_after_observed_floor(
                Some(&signed),&wrong,&tip(4,3),Some(&original_floor)),
                Err(PairWitnessError::WrongWitnessOrNonce));
            std::process::exit(73);
        }
        "tampered_signature"=>{
            signed.signature_ed25519_hex="0".repeat(128);
            assert_eq!(verify_signed_pair_latest_after_observed_floor(
                Some(&signed),&pin,&tip(4,3),Some(&original_floor)),
                Err(PairWitnessError::InvalidSignature));
            std::process::exit(74);
        }
        _=>panic!("unknown test PID role"),
    }
}
#[test]
fn signed_pair_two_pids_and_stale_response_fail_closed(){
    let root=test_root();
    let key=PKey::generate_ed25519().unwrap();
    put_new(&root.join("test_signer_key"),&key.raw_private_key().unwrap());
    put_new(&root.join("test_pinned_public_key"),&key.raw_public_key().unwrap());
    let run=|role:&str,code:i32|{
        let child=Command::new(env::current_exe().unwrap())
            .arg("--exact").arg("process_pair_witness_worker")
            .env("ASTRA_PAIR_CHILD_ROLE",role)
            .env("ASTRA_PAIR_CHILD_DIR",&root)
            .stdout(Stdio::null()).stderr(Stdio::piped())
            .spawn().unwrap();
        let pid=child.id();
        assert_ne!(pid,std::process::id());
        let result=child.wait_with_output().unwrap();
        assert_eq!(result.status.code(),Some(code),
            "role={role} pid={pid}: {}",String::from_utf8_lossy(&result.stderr));
        pid
    };
    let sign_pid=run("sign",70);
    assert!(root.join("signed_reply.json").is_file());
    let verify_pid=run("verify",71);
    assert_ne!(sign_pid,verify_pid);
    run("replay",72);
    run("wrong_nonce",73);
    run("tampered_signature",74);
    fs::remove_dir_all(root).unwrap();
}
