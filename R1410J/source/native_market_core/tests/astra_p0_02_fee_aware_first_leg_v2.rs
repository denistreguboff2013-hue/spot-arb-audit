//! P0-02/P1-02: proposed BASE-fee accounting, not a persisted owner migration.
use spotarb_native_market_core::{
    authoritative_state::NativeEntryPlan,
    fill_economics::{LegFillLedger,FillEvent,decimal_to_atoms as a,DEFAULT_AVG_TOLERANCE_ATOMS as T},
    native_spot_prehedge::NativeSpotPreHedgeEvidence,
    native_trade2f_asset_fee::SpotFeeFill,
    native_trade2f3_fee_aware_first_leg_v2::inspect_first_leg_fee_preview_v2 as inspect,
    native_trading_config::NativeTradingConfig,
    native_hard_safety::{require_native_mutation_egress,NativeHardSafetyError},
};
const CONFIG:&str=include_str!("../config/native_trading.yaml");
struct Case{
    plan:NativeEntryPlan,first:NativeSpotPreHedgeEvidence,
    source:Vec<SpotFeeFill>,
    policy:spotarb_native_market_core::native_trading_config::NativeEntryPolicyBinding,
}
fn seeded(base_fee:bool)->Case{
    let q=a("0.09").unwrap();let px=a("100").unwrap();
    let base=if base_fee{"BNT"}else{"USDT"};
    let fee=if base_fee{"0.001"}else{"0.009"};
    let cost=a(fee).unwrap();
    let plan=NativeEntryPlan{
        base:"BNT".into(),spot_symbol:"BNT/USDT".into(),futures_symbol:"BNTUSDT".into(),
        created_at_ms:1_780_000_000_000,
        planned_qty_atoms_e18:q*1_000_000,order_quote_atoms_e18:10_000_000_000_000_000_000,
        spot_baseline_atoms_e18:0,safety_generation:7,
    };
    let policy=NativeTradingConfig::parse(CONFIG).unwrap()
        .bind_entry(&plan,px,a("101").unwrap(),7).unwrap();
    let mut ledger=LegFillLedger::new("okx","BNT/USDT","buy","spot-intent","spot-client",q).unwrap();
    ledger.apply_cumulative_truth(q,Some(px),true,Some("FILLED"),Some("201"),
        Some(1_780_000_000_100),T).unwrap();
    ledger.apply_fill(FillEvent{
        fill_id:"9001".into(),qty_atoms:q,price_atoms:px,
        fee_currency:Some(base.into()),
        fee_cost_quote_atoms:if base_fee{None}else{Some(cost as i128)},
        trade_time_ms:Some(1_780_000_000_100),
    },T).unwrap();
    // Legacy R19 ledger incorrectly regards a truthful BASE fee as "incomplete".
    assert_eq!(ledger.fee_evidence_complete,!base_fee);
    let signed_base=if base_fee{-(cost as i128)*1_000_000}else{0};
    let net=if base_fee{q-cost}else{q};
    let first=NativeSpotPreHedgeEvidence{
        source_generation:2,observed_at_ms:1_780_000_001_000,
        safety_generation:7,spot_intent_id:"spot-intent".into(),
        spot_client_id:"spot-client".into(),spot_order_id:"201".into(),
        exact_ledger:ledger,signed_base_fee_atoms_e18:signed_base,
        observed_base_cash_atoms_e18:net*1_000_000,
        balance_updated_at_ms:1_780_000_000_900,
        fill_bill_ids:vec!["301".into()],filled_pages:2,
        cash_bracket_matched:true,
        exchange_bill_history_proven:false,
        exchange_global_completeness_proven:false,may_send_order:false,
    };
    let source=vec![SpotFeeFill{
        order_id:"201".into(),trade_id:"9001".into(),at_ms:1_780_000_000_100,
        side:"buy".into(),inst_id:"BNT-USDT".into(),
        price_quote_e12:px,gross_base_qty_e12:q,
        fee_ccy:base.into(),signed_fee_e12:-(cost as i128),
    }];
    Case{first,source,plan,policy}
}
fn v2_ordinary_late_leg(venue:&str,side:&str,order:&str,id:&str,
 qty:u128,t:u64)->LegFillLedger{
 let symbol=if venue=="okx"{"BNT/USDT"}else{"BNTUSDT"};
 let mut l=LegFillLedger::new(venue,symbol,side,order,order,qty).unwrap();
 l.apply_cumulative_truth(qty,Some(a("100").unwrap()),true,
    Some("FILLED"),Some(order),Some(t),T).unwrap();
 l.apply_fill(FillEvent{fill_id:id.into(),qty_atoms:qty,
    price_atoms:a("100").unwrap(),fee_currency:Some("USDT".into()),
    fee_cost_quote_atoms:Some(a("0.001").unwrap() as i128),
    trade_time_ms:Some(t)},T).unwrap();
 l
}
#[test]
fn p102_v2_source_checkpoint_matches_ordinary_net_four_leg_without_trade_authority(){
 use spotarb_native_market_core::{
   native_trade2f3_first_leg_v2_checkpoint::prepare_first_leg_v2,
   native_trade2f3_fee_adjusted_cycle_gate::inspect_v2_first_leg_to_ordinary_four_leg_consistency as linked,
 };
 let c=seeded(true);
 let checkpoint=prepare_first_leg_v2(99,&c.policy,&c.plan,&c.first,&c.source).unwrap();
 let net=a("0.089").unwrap();
 let sell=v2_ordinary_late_leg("okx","sell","901","9002",net,1_780_000_004_000);
 let short=v2_ordinary_late_leg("binance","sell","601","1001",net,1_780_000_002_000);
 let cover=v2_ordinary_late_leg("binance","buy","902","1002",net,1_780_000_003_000);
 let fee=SpotFeeFill{order_id:"901".into(),trade_id:"9002".into(),
    at_ms:1_780_000_004_000,side:"sell".into(),inst_id:"BNT-USDT".into(),
    price_quote_e12:a("100").unwrap(),gross_base_qty_e12:net,
    fee_ccy:"USDT".into(),signed_fee_e12:-(a("0.001").unwrap() as i128)};
 let result=linked(&checkpoint,99,&sell,&short,&cover,&[fee.clone()]).unwrap();
 assert_eq!(result.spot_buy_gross_atoms_e12,a("0.09").unwrap());
 assert_eq!(result.spot_inventory_delivered_atoms_e12,net);
 assert_eq!(result.base_fee_atoms_e12,a("0.001").unwrap());
 assert_eq!(result.spot_sell_gross_atoms_e12,net);
 assert_eq!(result.futures_short_open_atoms_e12,net);
 assert!(!result.may_send_cancel_retry_or_verify_closed);
 assert!(linked(&checkpoint,98,&sell,&short,&cover,&[fee.clone()]).is_err());
 let over=v2_ordinary_late_leg("binance","sell","601","1001",a("0.09").unwrap(),1_780_000_002_000);
 assert!(linked(&checkpoint,99,&sell,&over,&cover,&[fee.clone()]).is_err());
 let under=v2_ordinary_late_leg("binance","buy","902","1002",a("0.088").unwrap(),1_780_000_003_000);
 assert!(linked(&checkpoint,99,&sell,&short,&under,&[fee.clone()]).is_err());
 let mut wrong=fee.clone();wrong.trade_id="FOREIGN".into();
 assert!(linked(&checkpoint,99,&sell,&short,&cover,&[wrong]).is_err());
 assert!(linked(&checkpoint,99,&sell,&short,&cover,&[]).is_err());
 assert_eq!(require_native_mutation_egress(),Err(NativeHardSafetyError::MutationCapabilityNotCertified));
}
#[test]
fn p102_quote_fee_v2_checkpoint_matches_full_gross_ordinary_cycle(){
 use spotarb_native_market_core::{
  native_trade2f3_first_leg_v2_checkpoint::prepare_first_leg_v2,
  native_trade2f3_fee_adjusted_cycle_gate::inspect_v2_first_leg_to_ordinary_four_leg_consistency as linked,
 };
 let c=seeded(false);
 let v=prepare_first_leg_v2(99,&c.policy,&c.plan,&c.first,&c.source).unwrap();
 let q=a("0.09").unwrap();
 let sell=v2_ordinary_late_leg("okx","sell","901","9002",q,1_780_000_004_000);
 let short=v2_ordinary_late_leg("binance","sell","601","1001",q,1_780_000_002_000);
 let cover=v2_ordinary_late_leg("binance","buy","902","1002",q,1_780_000_003_000);
 let fee=SpotFeeFill{order_id:"901".into(),trade_id:"9002".into(),
    at_ms:1_780_000_004_000,side:"sell".into(),inst_id:"BNT-USDT".into(),
    price_quote_e12:a("100").unwrap(),gross_base_qty_e12:q,
    fee_ccy:"USDT".into(),signed_fee_e12:-(a("0.001").unwrap() as i128)};
 let r=linked(&v,99,&sell,&short,&cover,&[fee]).unwrap();
 assert_eq!(r.base_fee_atoms_e12,0);
 assert_eq!(r.spot_inventory_delivered_atoms_e12,q);
 assert_eq!(r.futures_short_open_atoms_e12,q);
 assert!(!r.may_send_cancel_retry_or_verify_closed);
}
#[test]
fn base_fee_hypothetical_first_leg_net_exact_risk_limited_but_not_persisted(){
    let c=seeded(true);
    // Old policy/owner C2 cannot persist this case. V2 diagnostics must NOT
    // silently satisfy old authority or lie about exchange attestation.
    assert!(c.policy.verify_exact_spot_fills(&c.plan,&c.first.exact_ledger).is_err());
    let r=inspect(&c.policy,&c.plan,&c.first,&c.source).unwrap();
    assert_eq!(r.original_spot_gross_e12,a("0.09").unwrap());
    assert_eq!(r.original_base_buy_fee_e12,a("0.001").unwrap());
    assert_eq!(r.original_delivered_base_e12,a("0.089").unwrap());
    assert_eq!(r.quote_spent_with_quote_fees_e12,a("9").unwrap());
    assert!(r.base_fee_is_explicitly_source_shaped);
    assert!(!r.persisted_owner_v2_seal_available);
    assert!(!r.independent_live_exchange_attestation);
    assert!(!r.trading_or_close_authorized);
    assert_eq!(require_native_mutation_egress(),Err(NativeHardSafetyError::MutationCapabilityNotCertified));
}
#[test]
fn quote_fee_legacy_case_retains_original_risk_reserve(){
    let c=seeded(false);
    assert!(c.policy.verify_exact_spot_fills(&c.plan,&c.first.exact_ledger).is_ok());
    let r=inspect(&c.policy,&c.plan,&c.first,&c.source).unwrap();
    assert_eq!(r.original_base_buy_fee_e12,0);
    assert_eq!(r.original_quote_buy_fee_e12,a("0.009").unwrap());
    assert_eq!(r.quote_spent_with_quote_fees_e12,a("9.009").unwrap());
    assert_eq!(r.original_delivered_base_e12,a("0.09").unwrap());
}
#[test]
fn original_base_fee_and_observed_cash_must_match_source_to_the_atom(){
    let mut c=seeded(true);c.first.signed_base_fee_atoms_e18=0;
    assert!(inspect(&c.policy,&c.plan,&c.first,&c.source).is_err());
    let mut c=seeded(true);c.first.observed_base_cash_atoms_e18+=1;
    assert!(inspect(&c.policy,&c.plan,&c.first,&c.source).is_err());
}
#[test]
fn missing_or_duplicate_source_is_denied(){
    let mut c=seeded(true);c.source.clear();
    assert!(inspect(&c.policy,&c.plan,&c.first,&c.source).is_err());
    let mut c=seeded(true);c.source.push(c.source[0].clone());
    assert!(inspect(&c.policy,&c.plan,&c.first,&c.source).is_err());
}
#[test]
fn falsified_fee_currency_or_quote_zero_conversion_is_denied(){
    let mut c=seeded(true);c.source[0].fee_ccy="USDT".into();
    assert!(inspect(&c.policy,&c.plan,&c.first,&c.source).is_err());
    let mut c=seeded(true);c.first.exact_ledger.fills.get_mut("9001").unwrap().fee_cost_quote_atoms=Some(0);
    assert!(inspect(&c.policy,&c.plan,&c.first,&c.source).is_err());
    let mut c=seeded(true);c.source[0].fee_ccy="BNB".into();
    assert!(inspect(&c.policy,&c.plan,&c.first,&c.source).is_err());
}
#[test]
fn unrelated_foreign_original_fill_and_wrong_order_id_denied(){
    let mut c=seeded(true);c.source[0].trade_id="FOREIGN".into();
    assert!(inspect(&c.policy,&c.plan,&c.first,&c.source).is_err());
    let mut c=seeded(true);c.source[0].order_id="WRONG".into();
    assert!(inspect(&c.policy,&c.plan,&c.first,&c.source).is_err());
}
#[test]
fn source_rebate_wrong_quantity_or_timestamp_denied(){
    let mut c=seeded(true);c.source[0].signed_fee_e12=a("0.001").unwrap() as i128;
    assert!(inspect(&c.policy,&c.plan,&c.first,&c.source).is_err());
    let mut c=seeded(true);c.source[0].gross_base_qty_e12=a("0.08").unwrap();
    assert!(inspect(&c.policy,&c.plan,&c.first,&c.source).is_err());
    let mut c=seeded(true);c.source[0].at_ms=0;
    assert!(inspect(&c.policy,&c.plan,&c.first,&c.source).is_err());
}
#[test]
fn overspending_original_configured_ten_dollar_budget_fails_closed(){
    let mut c=seeded(true);
    c.source[0].price_quote_e12=a("200").unwrap();
    c.first.exact_ledger.fills.get_mut("9001").unwrap().price_atoms=a("200").unwrap();
    assert!(inspect(&c.policy,&c.plan,&c.first,&c.source).is_err());
}


// Astra V2 durable SOURCE checkpoint: these tests are ordinary linked Rust
// integration tests. They cannot access the owner trading/mutation bypass.
mod durable_fee_checkpoint_tests {
    use super::*;
    use std::{fs,io::ErrorKind,path::PathBuf,
        sync::atomic::{AtomicU64,Ordering}};
    use spotarb_native_market_core::native_trade2f3_first_leg_v2_checkpoint::{
        prepare_first_leg_v2,persist_v2_once,load_v2,
        CheckpointFailpoint as FP,FirstLegCheckpointError as ErrV2,
    };
    static COUNTER:AtomicU64=AtomicU64::new(0);
    fn directory(tag:&str)->PathBuf{
        let n=COUNTER.fetch_add(1,Ordering::Relaxed);
        let p=std::env::temp_dir().join(format!(
            "astra_v2_fee_checkpoint_{}_{}_{}",std::process::id(),tag,n));
        fs::create_dir(&p).unwrap();p
    }
    fn prepare(base_fee:bool)->(Case,spotarb_native_market_core::native_trade2f3_first_leg_v2_checkpoint::FirstLegV2CheckpointPayload){
        let c=seeded(base_fee);
        let v=prepare_first_leg_v2(99,&c.policy,&c.plan,&c.first,&c.source).unwrap();
        (c,v)
    }
    #[test]
    fn base_fee_checkpoint_fsync_reopen_revalidates_all_sources_and_no_authority(){
        let (c,v)=prepare(true);
        let dir=directory("base");
        let old=dir.join("legacy_native_owner.json");
        fs::write(&old,b"unaltered-original-legacy-owner").unwrap();
        let path=dir.join("fee_v2.json");
        persist_v2_once(&path,&v,FP::None).unwrap();
        let (found,r)=load_v2(&path,99,&c.policy,&c.plan,&c.first).unwrap();
        assert_eq!(found,v);
        assert_eq!(r.original_spot_gross_e12,a("0.09").unwrap());
        assert_eq!(r.original_delivered_base_e12,a("0.089").unwrap());
        assert_eq!(r.original_base_buy_fee_e12,a("0.001").unwrap());
        assert_eq!(r.quote_spent_with_quote_fees_e12,a("9").unwrap());
        assert!(!found.original_owner_v2_seal_migrated);
        assert!(!found.exchange_global_fees_attested);
        assert!(!found.may_send_or_verify_closed);
        assert_eq!(fs::read(&old).unwrap(),b"unaltered-original-legacy-owner");
        assert_eq!(require_native_mutation_egress(),Err(NativeHardSafetyError::MutationCapabilityNotCertified));
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn quote_fee_checkpoint_roundtrips_without_converting_native_base_fee(){
        let (c,v)=prepare(false);
        let dir=directory("quote");let path=dir.join("source_v2.json");
        persist_v2_once(&path,&v,FP::None).unwrap();
        let (_,r)=load_v2(&path,99,&c.policy,&c.plan,&c.first).unwrap();
        assert_eq!(r.original_base_buy_fee_e12,0);
        assert_eq!(r.quote_spent_with_quote_fees_e12,a("9.009").unwrap());
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn second_identical_or_conflicting_write_is_denied_and_first_bytes_are_stable(){
        let (_,v)=prepare(true);
        let dir=directory("exclusive");let path=dir.join("source_v2.json");
        persist_v2_once(&path,&v,FP::None).unwrap();
        let original=fs::read(&path).unwrap();
        assert_eq!(persist_v2_once(&path,&v,FP::None),Err(ErrV2::Io(ErrorKind::AlreadyExists)));
        let mut other=v.clone();other.original_native_generation+=1;
        assert_eq!(persist_v2_once(&path,&other,FP::None),Err(ErrV2::Io(ErrorKind::AlreadyExists)));
        assert_eq!(fs::read(&path).unwrap(),original);
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn stale_generation_wrong_original_buy_or_plan_rejected_after_restart(){
        let (c,v)=prepare(true);
        let dir=directory("cas");let path=dir.join("source_v2.json");
        persist_v2_once(&path,&v,FP::None).unwrap();
        assert!(load_v2(&path,98,&c.policy,&c.plan,&c.first).is_err());
        let mut old=c.first.clone();old.observed_base_cash_atoms_e18+=1;
        assert!(load_v2(&path,99,&c.policy,&c.plan,&old).is_err());
        let mut plan=c.plan.clone();plan.safety_generation+=1;
        assert!(load_v2(&path,99,&c.policy,&plan,&c.first).is_err());
        let mut pol=c.policy.clone();pol.config_sha256.replace_range(0..1,"f");
        assert!(load_v2(&path,99,&pol,&c.plan,&c.first).is_err());
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn checksum_mutation_and_truncated_checkpoint_are_denied(){
        let (c,v)=prepare(true);
        let dir=directory("digest");let path=dir.join("source_v2.json");
        persist_v2_once(&path,&v,FP::None).unwrap();
        let saved=fs::read(&path).unwrap();
        let mut altered=saved.clone();
        // Rewrite just one content byte while keeping syntactically valid JSON.
        let key=b"\"89000000000\"";
        let index=altered.windows(key.len()).position(|w|w==key).unwrap();
        altered[index+2]=b'8';
        fs::write(&path,&altered).unwrap();
        assert_eq!(load_v2(&path,99,&c.policy,&c.plan,&c.first).unwrap_err(),ErrV2::Checksum);
        fs::write(&path,&saved[..saved.len()/2]).unwrap();
        assert!(load_v2(&path,99,&c.policy,&c.plan,&c.first).is_err());
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn v1_downgrade_or_authority_upgrade_never_persist_or_reopen(){
        let (c,v)=prepare(true);
        let dir=directory("schema");let path=dir.join("source_v2.json");
        let mut older=v.clone();older.schema_version=1;
        assert_eq!(persist_v2_once(&path,&older,FP::None),Err(ErrV2::Schema));
        assert!(!path.exists());
        let mut bad=v.clone();bad.may_send_or_verify_closed=true;
        assert!(persist_v2_once(&path,&bad,FP::None).is_err());
        let mut bad=v.clone();bad.exchange_global_fees_attested=true;
        assert!(persist_v2_once(&path,&bad,FP::None).is_err());
        let mut bad=v.clone();bad.original_owner_v2_seal_migrated=true;
        assert!(persist_v2_once(&path,&bad,FP::None).is_err());
        persist_v2_once(&path,&v,FP::None).unwrap();
        assert!(load_v2(&path,99,&c.policy,&c.plan,&c.first).is_ok());
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn crash_before_create_has_no_file_crash_after_partial_write_fail_closes(){
        let (c,v)=prepare(true);
        let dir=directory("powerloss");let path=dir.join("source_v2.json");
        assert_eq!(persist_v2_once(&path,&v,FP::BeforeCreate),
            Err(ErrV2::Io(ErrorKind::Interrupted)));
        assert!(!path.exists());
        assert_eq!(persist_v2_once(&path,&v,FP::AfterIncompleteWrite),
            Err(ErrV2::Io(ErrorKind::Interrupted)));
        assert!(path.exists());
        assert!(load_v2(&path,99,&c.policy,&c.plan,&c.first).is_err());
        assert_eq!(persist_v2_once(&path,&v,FP::None),
            Err(ErrV2::Io(ErrorKind::AlreadyExists)));
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn crash_after_file_fsync_before_directory_barrier_cannot_overwrite(){
        let (c,v)=prepare(true);
        let dir=directory("fsync");let path=dir.join("source_v2.json");
        assert_eq!(persist_v2_once(&path,&v,FP::AfterFileSyncBeforeDirSync),
            Err(ErrV2::Io(ErrorKind::Interrupted)));
        assert!(path.exists());
        assert!(load_v2(&path,99,&c.policy,&c.plan,&c.first).is_err(),
            "unacknowledged V2 checkpoint must quarantine even if its bytes are complete");
        assert_eq!(persist_v2_once(&path,&v,FP::None),
            Err(ErrV2::Io(ErrorKind::AlreadyExists)));
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn source_amount_mismatch_denied_before_creating_any_checkpoint(){
        let (_c,mut v)=prepare(true);
        let dir=directory("mismatch");let path=dir.join("source_v2.json");
        v.source_shaped_fee_fills[0].signed_fee_e12=-(a("0.002").unwrap() as i128);
        assert!(persist_v2_once(&path,&v,FP::None).is_err());
        assert!(!path.exists());
        let (_c,mut v)=prepare(true);
        v.quote_spent_atoms_e12="0".into();
        assert!(persist_v2_once(&path,&v,FP::None).is_err());
        assert!(!path.exists());
        fs::remove_dir_all(dir).unwrap();
    }
}


 // Astra next phase A: typed, strictly offline source-to-C3 net hedge target.
 // This is a proposed size, NOT a Binance fill or production owner CAS.
 mod v2_net_hedge_proposal_tests {
     use super::*;
     use spotarb_native_market_core::{
         native_trade2f3_first_leg_v2_checkpoint::prepare_first_leg_v2,
         native_trade2f3_v2_net_hedge_contract::{
             inspect_v2_net_hedge_proposal, V2NetHedgeError,
         },
     };
     fn prep(base_fee:bool)
       ->spotarb_native_market_core::native_trade2f3_first_leg_v2_checkpoint::FirstLegV2CheckpointPayload {
         let c=seeded(base_fee);
         prepare_first_leg_v2(99,&c.policy,&c.plan,&c.first,&c.source).unwrap()
     }
     fn proposal(
       v:&spotarb_native_market_core::native_trade2f3_first_leg_v2_checkpoint::FirstLegV2CheckpointPayload,
       generation:u64,
       qty:u128,
     )->Result<spotarb_native_market_core::native_trade2f3_v2_net_hedge_contract::V2NetHedgePreview,V2NetHedgeError>{
         inspect_v2_net_hedge_proposal(v,generation,"future-intent","future-client",qty)
     }
     #[test]
     fn base_fee_exact_net_short_offline_non_authorizing() {
         let v=prep(true);let r=proposal(&v,99,a("0.089").unwrap()).unwrap();
         assert_eq!(r.schema_version,2);
         assert_eq!(r.spot_buy_gross_atoms_e12,a("0.09").unwrap());
         assert_eq!(r.spot_base_fee_atoms_e12,a("0.001").unwrap());
         assert_eq!(r.spot_delivered_net_atoms_e12,a("0.089").unwrap());
         assert_eq!(r.proposed_futures_short_atoms_e12,a("0.089").unwrap());
         assert_eq!(r.original_trade_ids,vec!["9001"]);
         assert_eq!(r.original_bill_ids,vec!["301"]);
         assert_eq!(r.source_payload_sha256.len(),64);
         assert!(!r.persisted_owner_v2_seal_available);
         assert!(!r.owner_generation_cas_proven);
         assert!(!r.full_exchange_bill_ancestry_proven);
         assert!(!r.may_send_or_cancel_or_verify_closed);
         assert_eq!(require_native_mutation_egress(),Err(NativeHardSafetyError::MutationCapabilityNotCertified));
     }
     #[test]
     fn quote_fee_full_qty_stays_economic_not_real_permission() {
         let v=prep(false);let r=proposal(&v,99,a("0.09").unwrap()).unwrap();
         assert_eq!(r.spot_base_fee_atoms_e12,0);
         assert_eq!(r.spot_delivered_net_atoms_e12,a("0.09").unwrap());
         assert!(!r.may_send_or_cancel_or_verify_closed);
     }
     #[test]
     fn gross_hedge_overhedges_base_fee_and_is_denied() {
         let v=prep(true);
         assert!(proposal(&v,99,a("0.09").unwrap()).is_err());
         assert!(proposal(&v,99,0).is_err());
         assert!(proposal(&v,99,a("0.08").unwrap()).is_err());
     }
     #[test]
     fn old_or_mismatched_owner_generation_is_denied() {
         let v=prep(true);
         for g in [0,98,100]{assert!(proposal(&v,g,a("0.089").unwrap()).is_err());}
         let mut v=prep(true);v.original_native_generation=98;
         assert!(proposal(&v,99,a("0.089").unwrap()).is_err());
     }
     #[test]
     fn wrong_source_identity_and_currency_are_denied() {
         let mut v=prep(true);v.source_shaped_fee_fills[0].trade_id="FOREIGN".into();
         assert!(proposal(&v,99,a("0.089").unwrap()).is_err());
         let mut v=prep(true);v.source_shaped_fee_fills[0].fee_ccy="USDT".into();
         assert!(proposal(&v,99,a("0.089").unwrap()).is_err());
         let mut v=prep(true);v.source_shaped_fee_fills[0].signed_fee_e12=0;
         assert!(proposal(&v,99,a("0.089").unwrap()).is_err());
     }
     #[test]
     fn missing_duplicate_or_bad_bill_ids_do_not_create_a_hedge_proposal() {
         let mut v=prep(true);v.original.fill_bill_ids.clear();
         assert!(proposal(&v,99,a("0.089").unwrap()).is_err());
         let mut v=prep(true);v.original.fill_bill_ids[0]="".into();
         assert!(proposal(&v,99,a("0.089").unwrap()).is_err());
         let mut v=prep(true);v.original.fill_bill_ids.push("301".into());
         assert!(proposal(&v,99,a("0.089").unwrap()).is_err());
     }
     #[test]
     fn forged_authority_or_cached_net_quantity_is_denied() {
         let mut v=prep(true);v.may_send_or_verify_closed=true;
         assert!(proposal(&v,99,a("0.089").unwrap()).is_err());
         let mut v=prep(true);v.exchange_global_fees_attested=true;
         assert!(proposal(&v,99,a("0.089").unwrap()).is_err());
         let mut v=prep(true);v.original_owner_v2_seal_migrated=true;
         assert!(proposal(&v,99,a("0.089").unwrap()).is_err());
         let mut v=prep(true);v.net_base_after_fee_atoms_e12="90000000000".into();
         assert!(proposal(&v,99,a("0.089").unwrap()).is_err());
     }
     #[test]
     fn rejected_reused_spot_intent_or_client_identity() {
         let v=prep(true);
         let qty=a("0.089").unwrap();
         for (intent,client) in [
             ("spot-intent","future-client"),
             ("future-intent","spot-client"),
             ("","future-client"),
             ("future-intent",""),
         ] {
             assert!(inspect_v2_net_hedge_proposal(&v,99,intent,client,qty).is_err());
         }
     }
 }

 // P1-02/P1-04 V2: raw-like single OKX BASE bill + source trade fee ancestry.
 // All records are synthetic, never falsely construed as exchange attestation.
 mod v2_source_bill_ancestry_tests {
     use super::*;
     use spotarb_native_market_core::{
         native_trade2f3_first_leg_v2_checkpoint::{prepare_first_leg_v2,FirstLegV2CheckpointPayload},
         native_trade2f3_v2_bill_ancestry::{
             SourceShapedOkxBaseBillV2 as Bill,
             BoundedSourceBillWindowV2 as Window,
             inspect_v2_supplied_bill_ancestry as inspect,
         },
         native_trade2f_receipt_binding::adapt_v2_original_buy_bills_for_review as adapt,
     };
     fn fixture(base_fee:bool)->(FirstLegV2CheckpointPayload,Window,u128){
         let c=seeded(base_fee);
         let p=prepare_first_leg_v2(99,&c.policy,&c.plan,&c.first,&c.source).unwrap();
         let net=if base_fee{a("0.089").unwrap()}else{a("0.09").unwrap()};
         let delta=net as i128;
         let bill=Bill{
             bill_id:"301".into(),trade_id:"9001".into(),order_id:"201".into(),
             inst_id:"BNT-USDT".into(),ccy:"BNT".into(),sub_type:"1".into(),
             event_at_ms:1_780_000_000_200,
             gross_base_qty_e12:a("0.09").unwrap(),
             fee_ccy:if base_fee{"BNT"}else{"USDT"}.into(),
             signed_fee_e12:if base_fee{-(a("0.001").unwrap() as i128)}else{-(a("0.009").unwrap() as i128)},
             balance_change_base_e12:delta,balance_after_base_e12:net,
         };
         let w=Window{
             query_from_ms:c.plan.created_at_ms,
             query_through_ms:c.first.balance_updated_at_ms,
             captured_at_ms:c.first.observed_at_ms+100,
             page_count:2,empty_terminal_page_observed:true,
             source_shaped_base_bills_desc:vec![bill],
         };
         (p,w,net)
     }
     #[test]
     fn v2_original_source_buy_bill_adapts_once_with_no_fee_ccy_inference(){
         let (p,w,q)=fixture(true);
         let mapped=adapt(&p,99,&w,q).unwrap();
         assert_eq!(mapped.original_owner_generation,99);
         assert_eq!(mapped.original_buy_bills.len(),1);
         let b=&mapped.original_buy_bills[0];
         assert_eq!(b.bill_id,"301");
         assert_eq!(b.trade_id,"9001");
         assert_eq!(b.order_id,"201");
         assert_eq!(b.currency,"BNT");
         assert_eq!(b.signed_change_e12,a("0.089").unwrap() as i128);
         assert_eq!(b.observed_signed_fee_without_sourced_ccy_e12,
                    Some(-(a("0.001").unwrap() as i128)));
         assert!(!mapped.independent_exchange_account_authenticated);
         assert!(!mapped.full_owner_universe_proven);
         assert!(!mapped.may_trade_real);
         let (p,w,q)=fixture(false);
         let quote_fee=adapt(&p,99,&w,q).unwrap();
         assert_eq!(quote_fee.original_buy_bills.len(),1);
         assert_eq!(quote_fee.original_buy_bills[0].currency,"BNT");
         assert_eq!(quote_fee.original_buy_bills[0].signed_change_e12,
                    a("0.09").unwrap() as i128);
     }
     #[test]
     fn v2_original_bill_adapter_denies_wrong_owner_and_fictional_source(){
         let (p,w,q)=fixture(true);
         assert!(adapt(&p,100,&w,q).is_err(),"foreign Native generation");
         let mut changed=w.clone();
         changed.source_shaped_base_bills_desc[0].bill_id="302".into();
         assert!(adapt(&p,99,&changed,q).is_err(),"substituted original bill ID");
         let mut changed=w.clone();
         changed.source_shaped_base_bills_desc[0].signed_fee_e12=0;
         assert!(adapt(&p,99,&changed,q).is_err(),"changed original fee");
         let mut changed=w.clone();
         changed.source_shaped_base_bills_desc.push(
             changed.source_shaped_base_bills_desc[0].clone());
         assert!(adapt(&p,99,&changed,q).is_err(),"fabricated second bill");
     }
     #[test]
     fn fee_in_base_one_real_bill_shape_net_cash_and_proposed_short(){
         let (p,w,q)=fixture(true);
         let r=inspect(&p,99,&w,q).unwrap();
         assert_eq!(r.confirmed_source_bill_ids,vec!["301"]);
         assert_eq!(r.delivered_base_e12,a("0.089").unwrap());
         assert_eq!(r.proposed_futures_short_e12,a("0.089").unwrap());
         assert!(r.source_window_locally_consistent);
         assert!(!r.independent_exchange_bills_authenticated);
         assert!(!r.account_global_history_proven);
         assert!(!r.persisted_owner_v2_seal_proven);
         assert!(!r.actual_futures_filled_proven);
         assert!(!r.may_send_or_verify_closed);
         assert_eq!(require_native_mutation_egress(),Err(NativeHardSafetyError::MutationCapabilityNotCertified));
     }
     #[test]
     fn quote_fee_requires_no_fictional_second_account_bill(){
         let (p,w,q)=fixture(false);
         let r=inspect(&p,99,&w,q).unwrap();
         assert_eq!(r.confirmed_source_bill_ids.len(),1);
         assert_eq!(r.delivered_base_e12,a("0.09").unwrap());
     }
     #[test]
     fn absent_empty_page_or_missing_bill_remains_unknown(){
         let (p,mut w,q)=fixture(true);w.empty_terminal_page_observed=false;
         assert!(inspect(&p,99,&w,q).is_err());
         let (p,mut w,q)=fixture(true);w.source_shaped_base_bills_desc.clear();
         assert!(inspect(&p,99,&w,q).is_err());
         let (p,mut w,q)=fixture(true);w.page_count=0;
         assert!(inspect(&p,99,&w,q).is_err());
         let (p,mut w,q)=fixture(true);w.page_count=17;
         assert!(inspect(&p,99,&w,q).is_err());
     }
     #[test]
     fn duplicate_foreign_or_wrong_bill_trade_identity_denied(){
         let (p,mut w,q)=fixture(true);w.source_shaped_base_bills_desc.push(w.source_shaped_base_bills_desc[0].clone());
         assert!(inspect(&p,99,&w,q).is_err());
         let (p,mut w,q)=fixture(true);w.source_shaped_base_bills_desc[0].bill_id="302".into();
         assert!(inspect(&p,99,&w,q).is_err());
         let (p,mut w,q)=fixture(true);w.source_shaped_base_bills_desc[0].trade_id="FOREIGN".into();
         assert!(inspect(&p,99,&w,q).is_err());
         let (p,mut w,q)=fixture(true);w.source_shaped_base_bills_desc[0].order_id="OTHER".into();
         assert!(inspect(&p,99,&w,q).is_err());
     }
     #[test]
     fn mismatched_fee_asset_sign_or_qty_denied(){
         let (p,mut w,q)=fixture(true);w.source_shaped_base_bills_desc[0].fee_ccy="USDT".into();
         assert!(inspect(&p,99,&w,q).is_err());
         let (p,mut w,q)=fixture(true);w.source_shaped_base_bills_desc[0].signed_fee_e12=0;
         assert!(inspect(&p,99,&w,q).is_err());
         let (p,mut w,q)=fixture(true);w.source_shaped_base_bills_desc[0].gross_base_qty_e12+=1;
         assert!(inspect(&p,99,&w,q).is_err());
         let (p,mut w,q)=fixture(true);w.source_shaped_base_bills_desc[0].ccy="USDT".into();
         assert!(inspect(&p,99,&w,q).is_err());
     }
     #[test]
     fn cash_bracket_or_unrelated_base_movement_denied(){
         let (p,mut w,q)=fixture(true);w.source_shaped_base_bills_desc[0].balance_change_base_e12+=1;
         assert!(inspect(&p,99,&w,q).is_err());
         let (p,mut w,q)=fixture(true);w.source_shaped_base_bills_desc[0].balance_after_base_e12+=1;
         assert!(inspect(&p,99,&w,q).is_err());
         let (mut p,w,q)=fixture(true);p.original.observed_base_cash_atoms_e18+=1_000_000;
         assert!(inspect(&p,99,&w,q).is_err());
     }
     #[test]
     fn stale_owner_generation_or_futures_gross_overhedge_denied(){
         let (p,w,q)=fixture(true);
         assert!(inspect(&p,98,&w,q).is_err());
         assert!(inspect(&p,99,&w,a("0.09").unwrap()).is_err());
         assert!(inspect(&p,99,&w,q-1).is_err());
     }
     #[test]
     fn malformed_time_window_future_and_event_delay_denied(){
         let (p,mut w,q)=fixture(true);w.captured_at_ms=p.original.observed_at_ms+60_001;
         assert!(inspect(&p,99,&w,q).is_err());
         let (p,mut w,q)=fixture(true);w.query_from_ms+=1;
         assert!(inspect(&p,99,&w,q).is_err());
         let (p,mut w,q)=fixture(true);w.source_shaped_base_bills_desc[0].event_at_ms-=200;
         assert!(inspect(&p,99,&w,q).is_err());
         let (p,mut w,q)=fixture(true);w.source_shaped_base_bills_desc[0].sub_type="2".into();
         assert!(inspect(&p,99,&w,q).is_err());
     }
 }

 // V2 owner: ONE atomic SOURCE + BILLS + HEDGE review object.
 // No actual NativeStateStore V1 mutation or exchange network authority.
 mod v2_owner_genesis_review_tests {
     use super::*;
     use std::{
         fs,path::{Path,PathBuf},
         sync::{Arc,Barrier,atomic::{AtomicU64,Ordering}},
         thread,
     };
     use spotarb_native_market_core::{
         authoritative_state::{NativeAuthoritativeState,NativeStateStore},
         native_trade2f3_first_leg_v2_checkpoint::prepare_first_leg_v2,
         native_trade2f3_v2_bill_ancestry::{
             BoundedSourceBillWindowV2 as Window,SourceShapedOkxBaseBillV2 as Bill,
         },
         native_trade2f3_v2_owner_genesis::{
             prepare_v2_genesis,persist_genesis_once,load_genesis,
             V2OwnerGenesisReview as Owner,GenesisFailpoint as FP,
             V2GenesisError as Error,
         },
     };
     static SEQ:AtomicU64=AtomicU64::new(0);
     fn dir()->PathBuf{
         let n=SEQ.fetch_add(1,Ordering::Relaxed);
         let p=std::env::temp_dir().join(format!(
             "spotarb_v2_owner_review_{}_{}",std::process::id(),n));
         fs::create_dir(&p).unwrap();p
     }
     fn file(p:&Path)->PathBuf{p.join("owner_v2_review.json")}
     fn stage(p:&Path)->PathBuf{p.join("owner_v2_review.json.incomplete")}
     pub(super) fn example(base_fee:bool)->Owner{
         let c=seeded(base_fee);
         let cp=prepare_first_leg_v2(99,&c.policy,&c.plan,&c.first,&c.source).unwrap();
         let net=if base_fee{a("0.089").unwrap()}else{a("0.09").unwrap()};
         let raw=Bill{
             bill_id:"301".into(),trade_id:"9001".into(),order_id:"201".into(),
             inst_id:"BNT-USDT".into(),ccy:"BNT".into(),sub_type:"1".into(),
             event_at_ms:1_780_000_000_200,
             gross_base_qty_e12:a("0.09").unwrap(),
             fee_ccy:if base_fee{"BNT"}else{"USDT"}.into(),
             signed_fee_e12:if base_fee{-(a("0.001").unwrap() as i128)}else{-(a("0.009").unwrap() as i128)},
             balance_change_base_e12:net as i128,
             balance_after_base_e12:net,
         };
         let win=Window{
             query_from_ms:c.plan.created_at_ms,
             query_through_ms:c.first.balance_updated_at_ms,
             captured_at_ms:c.first.observed_at_ms+100,
             page_count:2,empty_terminal_page_observed:true,
             source_shaped_base_bills_desc:vec![raw],
         };
         prepare_v2_genesis(cp,win,"new-futures-intent","new-futures-client",net).unwrap()
     }
     #[test]
     fn one_sealed_owner_object_restarts_exact_base_fee_and_denies_execution(){
         let p=dir();let f=file(&p);let owner=example(true);
         persist_genesis_once(&f,&owner,FP::None).unwrap();
         assert!(!stage(&p).exists());
         let reopened=load_genesis(&f,99).unwrap();
         assert_eq!(reopened,owner);
         assert_eq!(reopened.schema_version,2);
         assert_eq!(reopened.owner_revision,1);
         assert_eq!(reopened.checkpoint.original.spot_order_id,"201");
         assert_eq!(reopened.original_okx_base_bill_window.source_shaped_base_bills_desc[0].bill_id,"301");
         assert_eq!(reopened.planned_net_short_atoms_e12,a("0.089").unwrap());
         assert!(!reopened.original_owner_cas_proven);
         assert!(!reopened.complete_exchange_account_authenticated);
         assert!(!reopened.trading_or_close_authorized);
         assert_eq!(require_native_mutation_egress(),Err(NativeHardSafetyError::MutationCapabilityNotCertified));
         fs::remove_dir_all(p).unwrap();
     }
     #[test]
     fn quote_fee_genesis_single_base_bill_reopen_without_derived_fake_bill(){
         let p=dir();let f=file(&p);let o=example(false);
         persist_genesis_once(&f,&o,FP::None).unwrap();
         let got=load_genesis(&f,99).unwrap();
         assert_eq!(got.planned_net_short_atoms_e12,a("0.09").unwrap());
         assert_eq!(got.original_okx_base_bill_window.source_shaped_base_bills_desc.len(),1);
         fs::remove_dir_all(p).unwrap();
     }
     #[test]
     fn failures_before_publish_quarantine_partial_stages_and_never_claim_ack(){
         let o=example(true);
         let d=dir();let f=file(&d);
         assert!(persist_genesis_once(&f,&o,FP::BeforeStage).is_err());
         assert!(!f.exists()&&!stage(&d).exists());
         persist_genesis_once(&f,&o,FP::None).unwrap();
         fs::remove_dir_all(d).unwrap();
         for fp in [FP::PartialStage,FP::AfterStageFileSync]{
             let d=dir();let f=file(&d);
             assert!(persist_genesis_once(&f,&o,fp).is_err());
             assert!(!f.exists()&&stage(&d).exists());
             assert!(load_genesis(&f,99).is_err());
             assert!(persist_genesis_once(&f,&o,FP::None).is_err());
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn published_before_dir_fsync_or_stage_cleanup_is_quarantined_not_acked(){
         let o=example(true);
         for fp in [FP::AfterPublishBeforeDirSync,FP::AfterParentSyncBeforeStageRemove]{
             let d=dir();let f=file(&d);
             assert!(persist_genesis_once(&f,&o,fp).is_err());
             assert!(f.exists()&&stage(&d).exists());
             assert!(load_genesis(&f,99).is_err());
             assert!(persist_genesis_once(&f,&o,FP::None).is_err());
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn tampered_truncated_and_rehashed_authority_claims_cannot_reopen(){
         let d=dir();let f=file(&d);let o=example(true);
         persist_genesis_once(&f,&o,FP::None).unwrap();
         let bytes=fs::read(&f).unwrap();
         fs::write(&f,&bytes[..bytes.len()/2]).unwrap();
         assert!(load_genesis(&f,99).is_err());
         fs::write(&f,&bytes).unwrap();
         // serde_json::Value would round large u128 policy values through f64.
         // Re-sign the typed object instead, preserving all original integer atoms.
         let mut forged=o.clone();forged.trading_or_close_authorized=true;
         let orig_hash=openssl::sha::sha256(&serde_json::to_vec(&o).unwrap())
             .iter().map(|b|format!("{b:02x}")).collect::<String>();
         let forged_hash=openssl::sha::sha256(&serde_json::to_vec(&forged).unwrap())
             .iter().map(|b|format!("{b:02x}")).collect::<String>();
         let original=String::from_utf8(bytes).unwrap();
         let altered=original.replacen(
             "\"trading_or_close_authorized\":false",
             "\"trading_or_close_authorized\":true",1);
         assert_ne!(altered,original);
         fs::write(&f,altered.as_bytes()).unwrap();
         assert!(load_genesis(&f,99).is_err());
         // A local attacker may rewrite both payload and checksum. No authority.
         let resigned=altered.replacen(&orig_hash,&forged_hash,1);
         assert_ne!(resigned,altered);
         fs::write(&f,resigned.as_bytes()).unwrap();
         assert!(matches!(load_genesis(&f,99),Err(Error::Untrusted(_))));
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn source_fee_missing_bill_foreign_trade_and_hedge_overfill_rejected_before_io(){
         for mode in 0..5 {
             let mut o=example(true);
             match mode{
                 0=>o.original_okx_base_bill_window.empty_terminal_page_observed=false,
                 1=>o.original_okx_base_bill_window.source_shaped_base_bills_desc[0].trade_id="FOREIGN".into(),
                 2=>o.original_okx_base_bill_window.source_shaped_base_bills_desc[0].fee_ccy="USDT".into(),
                 3=>o.planned_net_short_atoms_e12=a("0.09").unwrap(),
                 _=>o.checkpoint.source_shaped_fee_fills.clear(),
             }
             let d=dir();let f=file(&d);
             assert!(persist_genesis_once(&f,&o,FP::None).is_err());
             assert!(!f.exists()&&!stage(&d).exists());
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn stale_generation_schema_or_forged_owner_cas_denied(){
         let d=dir();let f=file(&d);let o=example(true);
         persist_genesis_once(&f,&o,FP::None).unwrap();
         for generation in [0,98,100] {
             assert!(load_genesis(&f,generation).is_err());
         }
         for mode in 0..3{
             let mut forged=example(true);
             match mode{
                 0=>forged.schema_version=1,
                 1=>forged.owner_revision=2,
                 _=>forged.original_owner_cas_proven=true,
             }
             assert!(persist_genesis_once(&d.join(format!("bad_{mode}.json")),&forged,FP::None).is_err());
         }
         assert!(persist_genesis_once(&f,&o,FP::None).is_err());
         assert_eq!(load_genesis(&f,99).unwrap(),o);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn competing_concurrent_publishers_only_one_wins(){
         let d=dir();let f=file(&d);
         let start=Arc::new(Barrier::new(5));
         let mut handles=Vec::new();
         for _ in 0..4{
             let p=f.clone();let barrier=start.clone();let o=example(true);
             handles.push(thread::spawn(move||{
                 barrier.wait();persist_genesis_once(&p,&o,FP::None).is_ok()
             }));
         }
         start.wait();
         let winners=handles.into_iter().map(|h|h.join().unwrap()).filter(|ok|*ok).count();
         assert_eq!(winners,1);
         assert_eq!(load_genesis(&f,99).unwrap().owner_revision,1);
         assert!(!stage(&d).exists());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn legacy_v1_owner_bytes_and_validator_unchanged_after_v2_created(){
         let d=dir();let v1=d.join("owner_v1.json");
         let old=NativeStateStore::open(&v1).unwrap();
         let legacy=NativeAuthoritativeState::empty_from_legacy("legacy-static-sha".into(),0);
         old.create(&legacy).unwrap();
         let original=fs::read(&v1).unwrap();
         let f=file(&d);persist_genesis_once(&f,&example(true),FP::None).unwrap();
         assert_eq!(fs::read(&v1).unwrap(),original);
         assert_eq!(old.load().unwrap(),Some(legacy));
         assert_eq!(load_genesis(&f,99).unwrap().owner_revision,1);
         fs::remove_dir_all(d).unwrap();
     }
     #[cfg(unix)]
     #[test]
     fn symlinked_parent_and_final_file_denied(){
         use std::os::unix::fs::symlink;
         let d=dir();let elsewhere=dir();let link=d.join("alias");
         symlink(&elsewhere,&link).unwrap();
         let o=example(true);
         assert!(persist_genesis_once(&link.join("owner.json"),&o,FP::None).is_err());
         let f=file(&d);let unrelated=d.join("unrelated");
         fs::write(&unrelated,b"not an owner").unwrap();
         symlink(&unrelated,&f).unwrap();
         assert!(persist_genesis_once(&f,&o,FP::None).is_err());
         assert!(load_genesis(&f,99).is_err());
         fs::remove_dir_all(d).unwrap();fs::remove_dir_all(elsewhere).unwrap();
     }
 }

 // Astra: actual NativeStateStore path/lock with EXPLICIT V1/V2 disk dispatch.
 // V2 only advances local proof REVIEW state: no spot/future send, no closed.
 mod v2_native_store_versioned_review_tests {
     use super::*;
     use super::v2_owner_genesis_review_tests::example;
     use spotarb_native_market_core::{
         authoritative_state::{NativeStateStore,NativeAuthoritativeState,NativeVersionedReview},
         native_trade2f3_v2_versioned_store::{
             V2ReviewPhase as Phase,V2ReviewFailpoint as FP,pending_path,
         },
     };
     use std::{fs,path::PathBuf,sync::{Arc,Barrier,atomic::{AtomicU64,Ordering}},thread};
     static NEXT:AtomicU64=AtomicU64::new(0);
     fn setup()->(PathBuf,NativeStateStore,PathBuf) {
         let dir=std::env::temp_dir().join(format!(
             "spotarb_v2_versioned_owner_{}_{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
         fs::create_dir(&dir).unwrap();
         let path=dir.join("native_authoritative_state.json");
         let s=NativeStateStore::open(&path).unwrap();
         (dir,s,path)
     }
     fn v2(s:&NativeStateStore)->spotarb_native_market_core::native_trade2f3_v2_versioned_store::V2ReviewState{
         match s.load_versioned_review().unwrap().unwrap(){
             NativeVersionedReview::FeeAwareV2(x)=>x,
             _=>panic!("must be explicit V2"),
         }
     }
     #[test]
     fn exact_v2_dispatch_same_owner_path_fsync_restart_and_no_legacy_downgrade(){
         let (d,s,_f)=setup();
         s.create_v2_review(example(true),FP::None).unwrap();
         assert!(s.load().is_err()); // V1 validator rejects V2 envelope.
         let x=v2(&NativeStateStore::open(d.join("native_authoritative_state.json")).unwrap());
         assert_eq!(x.revision,1);
         assert_eq!(x.genesis.original_owner_generation,99);
         assert_eq!(x.genesis.planned_net_short_atoms_e12,a("0.089").unwrap());
         assert_eq!(x.genesis.original_okx_base_bill_window.source_shaped_base_bills_desc[0].bill_id,"301");
         assert!(!x.may_send_or_cancel_or_verify_closed);
         assert!(!x.production_authorized);
         assert!(s.create(&NativeAuthoritativeState::empty_from_legacy("legacy".into(),0)).is_err());
         assert_eq!(require_native_mutation_egress(),Err(NativeHardSafetyError::MutationCapabilityNotCertified));
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn staged_v2_genesis_spot_fee_bill_hedge_revision_cas_and_cold_reopen(){
         let (d,s,p)=setup();
         s.create_v2_review(example(true),FP::None).unwrap();
         let before=fs::read(&p).unwrap();
         let r2=s.advance_v2_review(1,Phase::BillsReviewed,FP::None).unwrap();
         assert_eq!(r2.revision,2);
         assert!(s.advance_v2_review(1,Phase::HedgeSizeReviewed,FP::None).is_err());
         let r3=s.advance_v2_review(2,Phase::HedgeSizeReviewed,FP::None).unwrap();
         assert_eq!(r3.revision,3);
         assert_eq!(r3.genesis.planned_net_short_atoms_e12,a("0.089").unwrap());
         assert_eq!(v2(&NativeStateStore::open(&p).unwrap()),r3);
         assert_ne!(before,fs::read(&p).unwrap());
         assert!(s.advance_v2_review(3,Phase::BillsReviewed,FP::None).is_err());
         assert!(s.advance_v2_review(3,Phase::HedgeSizeReviewed,FP::None).is_err());
         assert_eq!(v2(&s).revision,3);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn recovery_only_remains_terminal_and_has_no_order_rights(){
         let (d,s,_p)=setup();
         s.create_v2_review(example(true),FP::None).unwrap();
         let q=s.advance_v2_review(1,Phase::RecoveryOnly,FP::None).unwrap();
         assert_eq!(q.phase,Phase::RecoveryOnly);
         assert!(!q.may_send_or_cancel_or_verify_closed);
         assert!(s.advance_v2_review(2,Phase::BillsReviewed,FP::None).is_err());
         assert!(s.advance_v2_review(2,Phase::RecoveryOnly,FP::None).is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn legacy_v1_reader_and_hash_remain_byte_identical_and_v2_cannot_upgrade_it(){
         let (d,s,p)=setup();
         let legacy=NativeAuthoritativeState::empty_from_legacy("source-hash".into(),0);
         s.create(&legacy).unwrap();
         let old=fs::read(&p).unwrap();
         assert_eq!(s.load_versioned_review().unwrap(),
             Some(NativeVersionedReview::LegacyV1(legacy.clone())));
         assert!(s.create_v2_review(example(true),FP::None).is_err());
         assert!(s.advance_v2_review(1,Phase::BillsReviewed,FP::None).is_err());
         assert_eq!(s.load().unwrap(),Some(legacy));
         assert_eq!(fs::read(&p).unwrap(),old);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn source_modified_fee_bill_and_forged_authority_do_not_create_v2_owner(){
         for m in 0..5 {
             let (d,s,p)=setup();let mut g=example(true);
             match m {
                 0=>g.checkpoint.source_shaped_fee_fills[0].fee_ccy="USDT".into(),
                 1=>g.original_okx_base_bill_window.source_shaped_base_bills_desc[0].trade_id="FOREIGN".into(),
                 2=>g.original_okx_base_bill_window.empty_terminal_page_observed=false,
                 3=>g.planned_net_short_atoms_e12=a("0.09").unwrap(),
                 _=>g.trading_or_close_authorized=true,
             }
             assert!(s.create_v2_review(g,FP::None).is_err());
             assert!(!p.exists());
             assert!(!pending_path(&p).exists());
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn interrupted_initial_write_quarantines_new_and_legacy_create_without_downgrade(){
         for fp in [FP::PartialWrite,FP::AfterFileSync,FP::AfterRename]{
             let (d,s,p)=setup();
             assert!(s.create_v2_review(example(true),fp).is_err());
             assert!(pending_path(&p).exists());
             assert!(s.load().is_err());
             assert!(s.load_versioned_review().is_err());
             assert!(s.create(&NativeAuthoritativeState::empty_from_legacy("legacy".into(),0)).is_err());
             assert!(s.create_v2_review(example(true),FP::None).is_err());
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn interrupted_update_before_rename_preserves_old_bytes_and_quarantines(){
         for fp in [FP::PartialWrite,FP::AfterFileSync]{
             let (d,s,p)=setup();s.create_v2_review(example(true),FP::None).unwrap();
             let original=fs::read(&p).unwrap();
             assert!(s.advance_v2_review(1,Phase::BillsReviewed,fp).is_err());
             assert_eq!(fs::read(&p).unwrap(),original);
             assert!(pending_path(&p).exists());
             assert!(s.load_versioned_review().is_err());
             assert!(s.advance_v2_review(1,Phase::BillsReviewed,FP::None).is_err());
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn post_rename_pre_directory_barrier_never_claims_ack_or_trade_capability(){
         let (d,s,p)=setup();s.create_v2_review(example(true),FP::None).unwrap();
         assert!(s.advance_v2_review(1,Phase::BillsReviewed,FP::AfterRename).is_err());
         // The rename is already visible; only a power-loss test can determine durability.
         let seen=v2(&NativeStateStore::open(&p).unwrap());
         assert_eq!(seen.revision,2);
         assert_eq!(seen.phase,Phase::BillsReviewed);
         assert!(!seen.may_send_or_cancel_or_verify_closed);
         assert!(!seen.production_authorized);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn concurrent_writers_one_revision_wins_with_native_store_original_flock(){
         let (d,s,p)=setup();s.create_v2_review(example(true),FP::None).unwrap();
         let sync=Arc::new(Barrier::new(6));
         let mut handles=Vec::new();
         for _ in 0..5 {
             let path=p.clone();let sync=sync.clone();
             handles.push(thread::spawn(move||{
                 let owner=NativeStateStore::open(path).unwrap();
                 sync.wait();
                 owner.advance_v2_review(1,Phase::BillsReviewed,FP::None).is_ok()
             }));
         }
         sync.wait();
         let successes=handles.into_iter().map(|h|h.join().unwrap()).filter(|v|*v).count();
         assert_eq!(successes,1);
         assert_eq!(v2(&s).revision,2);
         assert!(!pending_path(&p).exists());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn corrupted_version_unknown_envelope_and_invalid_source_never_fallback_to_v1(){
         let (d,s,p)=setup();
         s.create_v2_review(example(true),FP::None).unwrap();
         let original=fs::read(&p).unwrap();
         assert!(s.load().is_err());
         let text=String::from_utf8(original.clone()).unwrap();
         let broken=text.replacen("\"v2_schema\":2","\"v2_schema\":3",1);
         assert_ne!(text,broken);
         fs::write(&p,broken).unwrap();
         assert!(s.load_versioned_review().is_err());
         fs::write(&p,&original[..original.len()/2]).unwrap();
         assert!(s.load_versioned_review().is_err());
         let corrupt=text.replacen("\"BNT-USDT\"","\"FOREIGN-USDT\"",1);
         assert_ne!(text,corrupt);
         fs::write(&p,corrupt).unwrap();
         assert!(s.load_versioned_review().is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn local_snapshot_rollback_still_possible_without_external_monotonic_floor(){
         let (d,s,p)=setup();s.create_v2_review(example(true),FP::None).unwrap();
         let old=fs::read(&p).unwrap();
         s.advance_v2_review(1,Phase::BillsReviewed,FP::None).unwrap();
         s.advance_v2_review(2,Phase::HedgeSizeReviewed,FP::None).unwrap();
         assert_eq!(v2(&s).revision,3);
         // Explicit UNRESOLVED limit: administrator restores old intact V2 bytes.
         fs::write(&p,old).unwrap();
         assert_eq!(v2(&s).revision,1);
         assert!(!v2(&s).may_send_or_cancel_or_verify_closed);
         fs::remove_dir_all(d).unwrap();
     }
 }

 // Astra V2 C3: exact Binance opening executions under native owner CAS.
 // Synthetic source-shaped data only; no private GET, retry or send capability.
 mod v2_c3_native_owner_tests {
     use super::*;
     use super::v2_owner_genesis_review_tests::example;
     use spotarb_native_market_core::{
         authoritative_state::{NativeStateStore,NativeVersionedReview},
         native_trade2f3_v2_versioned_store::{
             V2ReviewPhase as Phase,V2ReviewFailpoint as FP,pending_path
         },
         native_trade2f3_v2_c3_futures_evidence::{
             V2C3FuturesObservation as C3,V2FuturesObservationKind as Kind,
             V2FuturesTrade as Trade,V2FuturesPositionSnapshot as Position,
             inspect_v2_c3_futures_observation as inspect,
         },
         native_hard_safety::{require_native_mutation_egress,NativeHardSafetyError},
     };
     use std::{fs,path::PathBuf,sync::{Arc,Barrier,atomic::{AtomicU64,Ordering}},thread};
     static SEQ:AtomicU64=AtomicU64::new(0);
     fn setup()->(PathBuf,NativeStateStore,PathBuf) {
         let d=std::env::temp_dir().join(format!(
             "astra_v2_c3_store_{}_{}",std::process::id(),SEQ.fetch_add(1,Ordering::Relaxed)));
         fs::create_dir(&d).unwrap();
         let p=d.join("owner.json");
         let s=NativeStateStore::open(&p).unwrap();
         s.create_v2_review(example(true),FP::None).unwrap();
         s.advance_v2_review(1,Phase::BillsReviewed,FP::None).unwrap();
         s.advance_v2_review(2,Phase::HedgeSizeReviewed,FP::None).unwrap();
         (d,s,p)
     }
     pub(super) fn observation(k:Kind)->C3{
         let amount=match k{
             Kind::ExactTargetShort=>a("0.089").unwrap(),
             Kind::PartialShort=>a("0.05").unwrap(),
             Kind::UnknownAcknowledgement=>0,
         };
         let unknown=k==Kind::UnknownAcknowledgement;
         let pos=Position{
             captured_at_ms:1_780_000_001_700,
             futures_symbol:"BNTUSDT".into(),
             short_qty_atoms_e12:amount,
             claimed_prior_short_atoms_e12:0,
         };
         C3{
             source_original_generation:99,source_owner_revision:3,
             futures_intent_id:"new-futures-intent".into(),
             futures_client_id:"new-futures-client".into(),
             futures_symbol:"BNTUSDT".into(),
             order_id:if unknown{None}else{Some("777".into())},
             captured_at_ms:1_780_000_002_000,
             kind:k,
             rows:if unknown{vec![]}else{vec![Trade{
                 trade_id:"88001".into(),order_id:"777".into(),
                 symbol:"BNTUSDT".into(),side:"SELL".into(),
                 position_side:"BOTH".into(),occurred_at_ms:1_780_000_001_500,
                 base_qty_atoms_e12:amount,price_quote_atoms_e12:a("100").unwrap(),
                 fee_currency:"USDT".into(),
                 signed_fee_quote_atoms_e12:-(a("0.002").unwrap() as i128),
                 realized_pnl_quote_atoms_e12:0,
             }]},
             first_position:if unknown{None}else{Some(pos.clone())},
             second_position:if unknown{None}else{Some(Position{
                 captured_at_ms:1_780_000_001_800,..pos
             })},
             queried_window_from_ms:1_780_000_000_000,
             queried_window_through_ms:1_780_000_002_000,
             all_pages_accounted_for:!unknown,
             terminal_empty_page_seen:!unknown,
             order_terminal_claimed:k==Kind::ExactTargetShort,
             exchange_global_history_authenticated:false,
             order_send_authorized:false,
             may_promote_hedged_or_verified_closed:false,
         }
     }
     fn reopened(s:&NativeStateStore)
         ->spotarb_native_market_core::native_trade2f3_v2_versioned_store::V2ReviewState{
         match s.load_versioned_review().unwrap().unwrap(){
             NativeVersionedReview::FeeAwareV2(v)=>v,_=>panic!("missing exact V2")
         }
     }
     #[test]
     fn exact_net_binance_short_stored_on_original_owner_revision_and_reopened(){
         let (d,s,p)=setup();
         let before=reopened(&s);
         let c=observation(Kind::ExactTargetShort);
         let preview=inspect(&before.genesis,&c).unwrap();
         assert_eq!(preview.target_short_atoms_e12,a("0.089").unwrap());
         assert_eq!(preview.remaining_exposure_atoms_e12,Some(0));
         assert!(!preview.independently_authenticated);
         let saved=s.record_v2_c3_review(3,c.clone(),FP::None).unwrap();
         assert_eq!(saved.revision,4);
         assert_eq!(saved.phase,Phase::FuturesEvidenceRecorded);
         let cold=NativeStateStore::open(&p).unwrap();
         assert_eq!(reopened(&cold),saved);
         assert_eq!(saved.c3_futures,Some(c));
         assert!(!saved.may_send_or_cancel_or_verify_closed);
         assert!(!saved.production_authorized);
         assert_eq!(require_native_mutation_egress(),Err(NativeHardSafetyError::MutationCapabilityNotCertified));
         assert!(s.load().is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn p103_c3_rejects_temporal_naked_short_even_when_final_qty_matches(){
         let (d,s,p)=setup();
         let before=fs::read(&p).unwrap();
         let genesis=reopened(&s).genesis;
         let ready=genesis.checkpoint.original.observed_at_ms
           .max(genesis.checkpoint.original.balance_updated_at_ms)
           .max(genesis.checkpoint.original.exact_ledger.last_fill_time_ms.unwrap());
         assert!(ready>0);
         let mut early=observation(Kind::ExactTargetShort);
         early.rows[0].occurred_at_ms=ready-1;
         assert!(inspect(&genesis,&early).is_err(),
             "final matched short does not excuse an earlier naked short");
         assert!(s.record_v2_c3_review(3,early,FP::None).is_err());
         let mut exact=observation(Kind::ExactTargetShort);
         exact.rows[0].occurred_at_ms=ready;
         assert!(inspect(&genesis,&exact).is_ok(),
             "short executed once spot settlement evidence exists is causal");
         assert_eq!(fs::read(&p).unwrap(),before);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn partial_short_remains_exposed_and_cannot_be_hedged_or_retried(){
         let (d,s,_p)=setup();let c=observation(Kind::PartialShort);
         let preview=inspect(&reopened(&s).genesis,&c).unwrap();
         assert_eq!(preview.observed_short_atoms_e12,Some(a("0.05").unwrap()));
         assert_eq!(preview.remaining_exposure_atoms_e12,Some(a("0.039").unwrap()));
         assert!(!preview.may_send_or_retry);
         let saved=s.record_v2_c3_review(3,c,FP::None).unwrap();
         assert_eq!(saved.phase,Phase::FuturesEvidenceRecorded);
         assert!(s.record_v2_c3_review(4,observation(Kind::ExactTargetShort),FP::None).is_err());
         let recovered=s.advance_v2_review(4,Phase::RecoveryOnly,FP::None).unwrap();
         assert_eq!(recovered.revision,5);
         assert!(!recovered.may_send_or_cancel_or_verify_closed);
         assert_eq!(reopened(&s),recovered);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn unknown_ack_is_not_zero_fill_and_no_resend_permission(){
         let (d,s,_p)=setup();
         let o=observation(Kind::UnknownAcknowledgement);
         let preview=inspect(&reopened(&s).genesis,&o).unwrap();
         assert!(!preview.locally_consistent);
         assert_eq!(preview.remaining_exposure_atoms_e12,None);
         assert_eq!(preview.observed_short_atoms_e12,None);
         assert!(!preview.may_send_or_retry);
         let saved=s.record_v2_c3_review(3,o,FP::None).unwrap();
         assert_eq!(saved.phase,Phase::FuturesEvidenceRecorded);
         assert!(s.record_v2_c3_review(3,observation(Kind::ExactTargetShort),FP::None).is_err());
         assert!(!reopened(&s).production_authorized);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn direct_transition_to_c3_without_source_is_not_allowed(){
         let (d,s,p)=setup();
         let bytes=fs::read(&p).unwrap();
         assert!(s.advance_v2_review(3,Phase::FuturesEvidenceRecorded,FP::None).is_err());
         assert_eq!(fs::read(&p).unwrap(),bytes);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn foreign_c3_account_side_order_ids_and_wrong_fee_denied_before_persist(){
         for mode in 0..11 {
             let (d,s,p)=setup();let mut o=observation(Kind::ExactTargetShort);
             match mode{
                 0=>o.source_original_generation=100,
                 1=>o.source_owner_revision=2,
                 2=>o.futures_intent_id="foreign-intent".into(),
                 3=>o.futures_client_id="foreign-client".into(),
                 4=>o.futures_symbol="ETHUSDT".into(),
                 5=>o.rows[0].order_id="998".into(),
                 6=>o.rows[0].side="BUY".into(),
                 7=>o.rows[0].position_side="SHORT".into(),
                 8=>o.rows[0].fee_currency="BNB".into(),
                 9=>o.rows[0].realized_pnl_quote_atoms_e12=-1,
                 _=>o.rows[0].trade_id="0001".into(),
             }
             let bytes=fs::read(&p).unwrap();
             assert!(s.record_v2_c3_review(3,o,FP::None).is_err(),"bad mode {}",mode);
             assert_eq!(fs::read(&p).unwrap(),bytes);
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn overhedge_underhedge_forged_snapshot_and_incomplete_pages_denied(){
         for mode in 0..11{
             let (d,s,p)=setup();let mut o=observation(Kind::ExactTargetShort);
             match mode{
                 0=>o.rows[0].base_qty_atoms_e12=a("0.09").unwrap(),
                 1=>o.rows[0].base_qty_atoms_e12=a("0.05").unwrap(),
                 2=>o.first_position.as_mut().unwrap().short_qty_atoms_e12+=1,
                 3=>o.second_position.as_mut().unwrap().claimed_prior_short_atoms_e12=1,
                 4=>o.all_pages_accounted_for=false,
                 5=>o.terminal_empty_page_seen=false,
                 6=>o.order_terminal_claimed=false,
                 7=>o.rows.push(o.rows[0].clone()),
                 8=>o.rows[0].signed_fee_quote_atoms_e12=1,
                 9=>o.first_position.as_mut().unwrap().captured_at_ms=1_780_000_001_000,
                 _=>o.second_position.as_mut().unwrap().captured_at_ms=1_780_000_001_700,
             }
             assert!(s.record_v2_c3_review(3,o,FP::None).is_err(),"bad mode {}",mode);
             assert!(pending_path(&p).symlink_metadata().is_err());
             assert_eq!(reopened(&s).revision,3);
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn false_unknown_with_order_or_position_truth_denied(){
         for mode in 0..4{
             let (d,s,_p)=setup();let mut o=observation(Kind::UnknownAcknowledgement);
             match mode{
                 0=>o.order_id=Some("777".into()),
                 1=>o.terminal_empty_page_seen=true,
                 2=>o.first_position=observation(Kind::PartialShort).first_position,
                 _=>o.all_pages_accounted_for=true,
             }
             assert!(s.record_v2_c3_review(3,o,FP::None).is_err());
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn partial_and_exact_classification_cannot_be_swapped(){
         let (d,s,_p)=setup();
         let mut partial=observation(Kind::PartialShort);
         partial.kind=Kind::ExactTargetShort;
         assert!(s.record_v2_c3_review(3,partial,FP::None).is_err());
         let mut exact=observation(Kind::ExactTargetShort);
         exact.kind=Kind::PartialShort;exact.order_terminal_claimed=false;
         assert!(s.record_v2_c3_review(3,exact,FP::None).is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn concurrent_c3_observations_one_writer_wins_and_source_not_replaced(){
         let (d,s,p)=setup();
         let sync=Arc::new(Barrier::new(6));
         let mut h=vec![];
         for n in 0..5{
             let f=p.clone();let barrier=sync.clone();
             h.push(thread::spawn(move||{
                 let st=NativeStateStore::open(f).unwrap();
                 let input=if n%2==0{observation(Kind::ExactTargetShort)}
                     else{observation(Kind::PartialShort)};
                 barrier.wait();
                 st.record_v2_c3_review(3,input,FP::None).is_ok()
             }));
         }
         sync.wait();
         assert_eq!(h.into_iter().map(|a|a.join().unwrap()).filter(|v|*v).count(),1);
         assert_eq!(reopened(&s).revision,4);
         assert!(reopened(&s).c3_futures.is_some());
         assert!(!reopened(&s).production_authorized);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn incomplete_fs_sync_write_requires_quarantine_not_blind_c3_ack(){
         for fp in [FP::PartialWrite,FP::AfterFileSync]{
             let (d,s,p)=setup();let before=fs::read(&p).unwrap();
             assert!(s.record_v2_c3_review(3,observation(Kind::ExactTargetShort),fp).is_err());
             assert_eq!(fs::read(&p).unwrap(),before);
             assert!(pending_path(&p).exists());
             assert!(s.load_versioned_review().is_err());
             assert!(s.record_v2_c3_review(3,observation(Kind::ExactTargetShort),FP::None).is_err());
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn rename_before_dir_fsync_yields_unknown_commit_status_no_resend(){
         let (d,s,p)=setup();
         assert!(s.record_v2_c3_review(3,observation(Kind::ExactTargetShort),FP::AfterRename).is_err());
         let a=reopened(&NativeStateStore::open(&p).unwrap());
         assert_eq!(a.revision,4);
         assert_eq!(a.phase,Phase::FuturesEvidenceRecorded);
         assert!(!a.may_send_or_cancel_or_verify_closed);
         assert!(!a.production_authorized);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn rollback_of_intact_snapshot_still_requires_independent_anchor(){
         let (d,s,p)=setup();let before=fs::read(&p).unwrap();
         s.record_v2_c3_review(3,observation(Kind::ExactTargetShort),FP::None).unwrap();
         assert_eq!(reopened(&s).revision,4);
         fs::write(&p,before).unwrap();
         assert_eq!(reopened(&s).revision,3);
         assert!(!reopened(&s).production_authorized);
         fs::remove_dir_all(d).unwrap();
     }
 }

 // Astra C5/C6/B4: additive owner-bound FOUR LEG fee-aware close, MOCK only.
 mod v2_four_leg_native_close_tests {
     use super::*;
     use super::v2_owner_genesis_review_tests::example;
     use super::v2_c3_native_owner_tests::observation;
     use spotarb_native_market_core::{
         authoritative_state::{NativeStateStore,NativeVersionedReview},
         native_trade2f3_v2_versioned_store::{
             V2ReviewState,V2ReviewPhase as Phase,V2ReviewFailpoint as FP,pending_path,
         },
         native_trade2f3_v2_c3_futures_evidence::V2FuturesObservationKind as Kind,
         native_trade2f3_v2_four_leg_close_review::{
             V2FourLegCloseObservation as Close,V2SpotSellFill as Spot,
             V2FuturesCoverFill as Cover,V2CloseFlatObservation as Flat,
             inspect_v2_four_leg_close as inspect,
         },
     };
     use std::{fs,path::PathBuf,sync::{Arc,Barrier,atomic::{AtomicU64,Ordering}},thread};
     static NEXT:AtomicU64=AtomicU64::new(0);
     pub(super) fn setup(kind:Kind)->(PathBuf,NativeStateStore,PathBuf){
         let d=std::env::temp_dir().join(format!(
             "astra_v2_4leg_{}_{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
         fs::create_dir(&d).unwrap();
         let p=d.join("owner.json");let s=NativeStateStore::open(&p).unwrap();
         s.create_v2_review(example(true),FP::None).unwrap();
         s.advance_v2_review(1,Phase::BillsReviewed,FP::None).unwrap();
         s.advance_v2_review(2,Phase::HedgeSizeReviewed,FP::None).unwrap();
         s.record_v2_c3_review(3,observation(kind),FP::None).unwrap();
         (d,s,p)
     }
     pub(super) fn read(s:&NativeStateStore)->V2ReviewState{
         match s.load_versioned_review().unwrap().unwrap(){
             NativeVersionedReview::FeeAwareV2(v)=>v,_=>panic!("expected V2"),
         }
     }
     pub(super) fn close()->Close{
         let qty=a("0.089").unwrap();let fee=a("0.002").unwrap() as i128;
         Close{
             source_original_generation:99,source_v2_revision:4,
             futures_cover_intent_id:"cover-futures-intent".into(),
             futures_cover_client_id:"cover-futures-client".into(),
             futures_cover_order_id:"778".into(),
             spot_sell_intent_id:"sell-spot-intent".into(),
             spot_sell_client_id:"sell-spot-client".into(),
             spot_sell_order_id:"202".into(),
             window_from_ms:1_780_000_002_000,
             window_through_ms:1_780_000_005_000,
             captured_at_ms:1_780_000_005_000,
             spot_sell_rows:vec![Spot{
                 trade_id:"9002".into(),bill_id:"302".into(),
                 order_id:"202".into(),symbol:"BNT-USDT".into(),
                 side:"sell".into(),occurred_at_ms:1_780_000_004_000,
                 base_qty_atoms_e12:qty,price_quote_atoms_e12:a("101").unwrap(),
                 fee_currency:"USDT".into(),
                 signed_fee_quote_atoms_e12:-(a("0.009").unwrap() as i128),
                 base_balance_delta_e12:-(qty as i128),base_balance_after_e12:0,
             }],
             futures_cover_rows:vec![Cover{
                 trade_id:"88002".into(),order_id:"778".into(),
                 symbol:"BNTUSDT".into(),side:"BUY".into(),position_side:"BOTH".into(),
                 occurred_at_ms:1_780_000_003_000,
                 base_qty_atoms_e12:qty,price_quote_atoms_e12:a("99").unwrap(),
                 fee_currency:"USDT".into(),
                 signed_fee_quote_atoms_e12:-fee,
                 realized_pnl_quote_atoms_e12:a("0.089").unwrap() as i128,
             }],
             first_futures_flat:Flat{captured_at_ms:1_780_000_003_400,
                 futures_symbol:"BNTUSDT".into(),short_qty_atoms_e12:0,
                 claimed_prior_short_atoms_e12:qty},
             second_futures_flat:Flat{captured_at_ms:1_780_000_003_500,
                 futures_symbol:"BNTUSDT".into(),short_qty_atoms_e12:0,
                 claimed_prior_short_atoms_e12:qty},
             spot_bill_window_complete:true,spot_terminal_page_seen:true,
             futures_trade_window_complete:true,futures_terminal_page_seen:true,
             both_closing_orders_terminal:true,
             independent_account_history_authenticated:false,
             late_funding_and_transfers_complete:false,
             trading_or_verified_closed_authorized:false,
         }
     }
     fn paper_report(
         intent:&str,client:&str,order:&str,
         id:&str,qty:u128,price:u128,fee_ccy:&str,fee:Option<i128>,when:u64,
     )->spotarb_native_market_core::native_paper_partial_recovery_gate::PaperOrderReport{
         use spotarb_native_market_core::fill_economics::FillEvent;
         spotarb_native_market_core::native_paper_partial_recovery_gate::PaperOrderReport{
             intent_id:intent.into(),client_order_id:client.into(),
             exchange_order_id:order.into(),cumulative_qty_atoms:qty,
             average_price_atoms:Some(price),terminal:true,status:"filled".into(),
             observed_at_ms:when,
             fills:vec![FillEvent{fill_id:id.into(),qty_atoms:qty,
                 price_atoms:price,fee_currency:Some(fee_ccy.into()),
                 fee_cost_quote_atoms:fee,trade_time_ms:Some(when)}],
         }
     }
     pub(super) fn build_v2_compatible_paper(
         v:&V2ReviewState,o:&Close,wrong_cover_price:bool,
     )->spotarb_native_market_core::native_paper_partial_recovery_gate::PaperPartialGate{
         use spotarb_native_market_core::native_paper_partial_recovery_gate::PaperPartialGate;
         let original=&v.genesis.checkpoint;
         let first=&original.source_shaped_fee_fills[0];
         let id=&original.original;
         let qty=a("0.089").unwrap();
         let mut g=PaperPartialGate::new(99,"v2_mock_owner_BNT","BNTUSDT",
             &id.spot_intent_id,&id.spot_client_id,first.gross_base_qty_e12).unwrap();
         g.observe_spot(&paper_report(&id.spot_intent_id,&id.spot_client_id,
            &id.spot_order_id,&first.trade_id,first.gross_base_qty_e12,
            first.price_quote_e12,&first.fee_ccy,None,first.at_ms)).unwrap();
         g.record_native_spot_fee(original.source_shaped_fee_fills.clone()).unwrap();
         let c3=v.c3_futures.as_ref().unwrap();
         let first_short=&c3.rows[0];
         g.begin_paper_short(&v.genesis.futures_intent_id,
            &v.genesis.futures_client_id,qty).unwrap();
         g.observe_short(&paper_report(&v.genesis.futures_intent_id,
            &v.genesis.futures_client_id,c3.order_id.as_deref().unwrap(),
            &first_short.trade_id,qty,first_short.price_quote_atoms_e12,
            "USDT",Some(-first_short.signed_fee_quote_atoms_e12),
            first_short.occurred_at_ms)).unwrap();
         let cover=&o.futures_cover_rows[0];
         g.begin_paper_cover(&o.futures_cover_intent_id,&o.futures_cover_client_id,qty).unwrap();
         g.observe_paper_cover(&paper_report(&o.futures_cover_intent_id,
            &o.futures_cover_client_id,&o.futures_cover_order_id,
            &cover.trade_id,qty,cover.price_quote_atoms_e12+
                u128::from(wrong_cover_price),
            "USDT",Some(-cover.signed_fee_quote_atoms_e12),
            cover.occurred_at_ms)).unwrap();
         let sell=&o.spot_sell_rows[0];
         g.begin_paper_sell(&o.spot_sell_intent_id,&o.spot_sell_client_id,qty).unwrap();
         g.observe_paper_sell(&paper_report(&o.spot_sell_intent_id,
            &o.spot_sell_client_id,&o.spot_sell_order_id,
            &sell.trade_id,qty,sell.price_quote_atoms_e12,
            "USDT",Some(-sell.signed_fee_quote_atoms_e12),
            sell.occurred_at_ms)).unwrap();
         g
     }
     pub(super) fn paper_owner_with_gate(
         folder:&std::path::Path,gate:spotarb_native_market_core::
            native_paper_partial_recovery_gate::PaperPartialGate,
     )->spotarb_native_market_core::native_paper_owner_transaction_review::PaperOwner{
         use spotarb_native_market_core::{
           native_paper_owner_transaction_review::{PaperOwner,OwnerError},
           native_paper_market_execution_review::{
             PaperMachine,PaperAssumptions,PaperPhase,PaperPosition,
           },
         };
         let mut owner=PaperOwner::start_new(&folder.join("paper.jsonl"),
            1_780_000_005_500).unwrap();
         owner.transact(move|portfolio,gates|{
             portfolio.stats.entries=1;
             let position=PaperPosition{
               base:"BNT".into(),signal_ns:1,spot_buy_ns:1,
               signal_estimated_net_bps:50.0,
               spot_gross_base:0.09,spot_delivered_base:0.089,
               spot_paid_quote:9.0,futures_short_ns:Some(2),
               futures_short_proceeds_quote:Some(8.9),
               futures_short_base:Some(0.089),
               hedge_entry_estimated_net_bps:Some(50.0),
             };
             portfolio.positions.insert("BNT".into(),PaperMachine{
               config:PaperAssumptions::default(),
               phase:PaperPhase::AwaitingSpotExit{
                 pos:position,cover_quote:8.8,cover_ns:3,
                 close_estimate_quote:0.1,
               },
             });
             gates.insert("BNT".into(),gate);
             Ok::<(),OwnerError>(())
         }).unwrap();
         owner
     }
     #[test]
     fn persisted_native_v2_owner_directory_lists_local_closure_and_unresolved(){
         use spotarb_native_market_core::native_paper_v2_close_source_archive_review::
             scan_local_v2_owner_directory as scan;
         let (folder,store,owner_file)=setup(Kind::ExactTargetShort);
         let root=folder.join("dedicated_local_v2_owner_files");
         fs::create_dir(&root).unwrap();
         let entry=root.join("owner_a.json");
         fs::copy(&owner_file,&entry).unwrap();
         let g=read(&store).genesis.original_owner_generation;
         let initial=scan(&root).unwrap();
         assert_eq!(initial.examined_owner_files,1);
         assert_eq!(initial.unresolved_owner_generations,vec![g]);
         assert!(initial.locally_reviewed_four_leg_generations.is_empty());
         assert!(!initial.source_account_authenticated);
         assert!(!initial.actual_closed_trades_proven);
         assert!(!initial.full_owner_history_proven);
         assert!(!initial.may_trade_real);
         store.record_v2_four_leg_close_review(4,close(),FP::None).unwrap();
         fs::copy(&owner_file,&entry).unwrap();
         let revised=scan(&root).unwrap();
         assert_eq!(revised.locally_reviewed_four_leg_generations,vec![g]);
         assert!(revised.unresolved_owner_generations.is_empty());
         assert!(!revised.actual_closed_trades_proven);
         let duplicate=root.join("owner_b.json");
         fs::copy(&entry,&duplicate).unwrap();
         assert!(scan(&root).is_err(),"duplicate Native generation denied");
         fs::remove_file(&duplicate).unwrap();
         fs::write(root.join("unexpected.txt"),b"not an owner").unwrap();
         assert!(scan(&root).is_err(),"unexpected file denied");
         fs::remove_file(root.join("unexpected.txt")).unwrap();
         store.advance_v2_review(5,Phase::RecoveryOnly,FP::None).unwrap();
         fs::copy(&owner_file,&entry).unwrap();
         let recovery=scan(&root).unwrap();
         assert_eq!(recovery.unresolved_owner_generations,vec![g]);
         assert!(recovery.locally_reviewed_four_leg_generations.is_empty());
         fs::remove_file(&entry).unwrap();
         assert!(scan(&root).is_err(),"empty directory is not complete");
         fs::remove_dir_all(&folder).unwrap();
     }
     #[test]
     fn r211_local_persisted_owner_and_complete_archive_directory_fee_day(){
         use spotarb_native_market_core::{
          native_paper_v2_close_source_archive_review::{
           write_local_close_source_once,LocalArchiveFailpoint,
           PersistedLocalArchivedCloseCandidate,
          },
          native_okx_persisted_fee_census_r211_review::
           inspect_persisted_okx_fee_directory_r211 as strict,
         };
         let (folder,store,owner_file)=setup(Kind::ExactTargetShort);
         let original=read(&store);
         let dir=folder.join("r211_archives");
         fs::create_dir(&dir).unwrap();
         let archived=dir.join("one.json");
         write_local_close_source_once(&archived,&original,&close(),
             LocalArchiveFailpoint::None).unwrap();
         let items=[PersistedLocalArchivedCloseCandidate{
             preclose_v2_file:&owner_file,archive_path:&archived,
         }];
         let policy=NativeTradingConfig::parse(CONFIG).unwrap();
         let audit=strict(&policy,1_780_000_006_000,
            "research-owner",&dir,&items,&[]).unwrap();
         assert!(audit.local_archive_directory_matches_inputs);
         assert_eq!(audit.local_archives_checked,1);
         assert_eq!(audit.fee_day.included_sell_quote_fee_fills,1);
         assert_eq!(audit.fee_day.quote_fee_day.known_quote_okx_fees_e12,
            a("0.009").unwrap());
         assert_eq!(audit.fee_day.unvalued_buy_base_fee_asset.as_deref(),
            Some("BNT"));
         assert_eq!(audit.fee_day.unvalued_today_buy_base_fee_atoms_e12,
            a("0.001").unwrap());
         assert!(!audit.account_wide_exchange_history_authenticated);
         assert!(!audit.complete_unresolved_owner_inventory_proven);
         assert!(!audit.day_fees_proven_complete);
         assert!(!audit.independently_verified_closed&&!audit.may_trade_real);
         fs::remove_dir_all(folder).unwrap();
     }
     // R138 mock replies are offline-only source-shaped original GETs.
     struct R138Okx{base_ms:u64,client:String}
     use spotarb_native_market_core::{
       private_https::{HttpExecutor,HttpsRequest,HttpResponse,TransportError},
       private_rest::HttpMethod,
     };
     impl HttpExecutor for R138Okx{
       fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
         assert_eq!(req.method,HttpMethod::Get);
         assert!(!req.mutation);
         let url=url::Url::parse(&req.url).unwrap();
         let q=url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
         let body=match url.path(){
           "/api/v5/account/balance"=>format!(
             r#"{{"code":"0","data":[{{"details":[{{"ccy":"BNT","cashBal":"1.125","availBal":"1.125","frozenBal":"0","uTime":"{}"}}]}}]}}"#,
             self.base_ms+2000),
           "/api/v5/trade/order"=>{
              assert_eq!(q.get("clOrdId").map(|v|v.as_ref()),Some(self.client.as_str()));
              format!(r#"{{"code":"0","data":[{{"ordId":"789","clOrdId":"{}","state":"filled","accFillSz":"1","avgPx":"100","fillTime":"{}"}}]}}"#,
                self.client,self.base_ms+320)
           },
           "/api/v5/trade/fills-history"=>{
              assert_eq!(q.get("ordId").map(|v|v.as_ref()),Some("789"));
              if q.get("after").is_some(){r#"{"code":"0","data":[]}"#.into()}
              else{format!(r#"{{"code":"0","data":[{{"ordId":"789","billId":"301","tradeId":"T1","fillPx":"100","fillSz":"1","fee":"-0.1","feeCcy":"USDT","fillTime":"{}"}}]}}"#,
                  self.base_ms+320)}
           },
           "/api/v5/account/bills-archive"=>{
              assert_eq!(q.get("ccy").map(|v|v.as_ref()),Some("BNT"));
              if q.get("after").is_some(){r#"{"code":"0","data":[]}"#.into()}
              else{format!(r#"{{"code":"0","data":[{{"ccy":"BNT","ordId":"789","instId":"BNT-USDT","subType":"1","tradeId":"T1","billId":"301","bal":"1.125","balChg":"1","sz":"1","fee":"-0.1","feeCcy":"USDT","ts":"{}"}}]}}"#,
                self.base_ms+350)}
           },
           "/api/v5/account/config"=>
              r#"{"code":"0","data":[{"acctLv":"2","posMode":"net_mode"}]}"#.into(),
           "/api/v5/account/positions"=>
              r#"{"code":"0","data":[]}"#.into(),
           "/api/v5/trade/orders-pending"=>
              r#"{"code":"0","data":[]}"#.into(),
           p=>panic!("R138 unexpected mock GET {p}"),
         };
         Ok(HttpResponse{status:200,headers:vec![],body})
       }
     }
     // R138_MOCK
     #[test]
     fn r138_c6_bills_futures_unknown_r211_cold_join(){
        use spotarb_native_market_core::{
          authoritative_state::{NativeAuthoritativeState,NativeEntryLeg,NativeEntryPhase},
          fill_economics::{PositionEconomicsState,EntryEconomicsState,
                            EntryTiming,LegFillAggregate},
          native_spot_prehedge::audit_okx_first_leg_before_hedge,
          native_spot_prehedge_bills::audit_okx_first_leg_cash_bills_window,
          private_rest::{OkxCredentials,OkxDomain},
          native_okx_persisted_fee_census_r211_review::
             inspect_native_pending_directory_r136 as join,
          native_paper_v2_close_source_archive_review::{
             write_local_close_source_once,LocalArchiveFailpoint,
             PersistedLocalArchivedCloseCandidate},
        };
        let (folder,store,owner_file)=setup(Kind::ExactTargetShort);
        let archive_dir=folder.join("r138-archives");
        fs::create_dir(&archive_dir).unwrap();
        let archive=archive_dir.join("one.json");
        write_local_close_source_once(&archive,&read(&store),&close(),
          LocalArchiveFailpoint::None).unwrap();
        let inputs=[PersistedLocalArchivedCloseCandidate{
          preclose_v2_file:&owner_file,archive_path:&archive,
        }];
        let config=NativeTradingConfig::parse(CONFIG).unwrap();
        let native=NativeStateStore::open(folder.join("r138-native.json")).unwrap();
        native.create(&NativeAuthoritativeState::empty_from_legacy(
          "33".repeat(32),0)).unwrap();
        let now=spotarb_native_market_core::private_https::wall_ms();
        let api=R138Okx{
          base_ms:now.saturating_sub(10_000),
          client:"sab00000000000000000000000000138".into(),
        };
        let creds=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
        let qty=a("1").unwrap();
        let econ=PositionEconomicsState{
          entry:EntryEconomicsState{
            signal_gross_microbps:100_000_000,
            signal_expected_net_microbps:80_000_000,
            signal_spot_price_atoms:Some(a("100").unwrap()),
            signal_futures_price_atoms:Some(a("101").unwrap()),
            projected_cost_reserve_microbps:20_000_000,
            spot:LegFillAggregate::new("okx","BNT/USDT","buy",qty).unwrap(),
            futures:LegFillAggregate::new("binance","BNTUSDT","sell",qty).unwrap(),
            timing:EntryTiming::default(),
            post_leg1_projected_gross_microbps:None,
            post_leg1_projected_net_microbps:None,
            locked_actual_gross_microbps:None,
            locked_actual_net_microbps:None,
            signal_to_leg1_slippage_microbps:None,
            signal_to_hedge_decay_microbps:None,
            execution_cost_vs_signal_microbps:None,
          },exit:None,
        };
        let mut plan=seeded(true).plan;
        plan.planned_qty_atoms_e18=1_000_000_000_000_000_000;
        plan.spot_baseline_atoms_e18=125_000_000_000_000_000;
        plan.order_quote_atoms_e18=10_000_000_000_000_000_000;
        plan.created_at_ms=api.base_ms;
        plan.safety_generation=7;
        let (_,permit)=native.begin_native_entry(1,plan,econ,
          "r138-spot", &api.client).unwrap();
        let s3=native.record_spot_entry_observation(
          permit,1_000_000_000_000_000_000,
          Some(a("100").unwrap()),true,false,false).unwrap();
        assert_eq!(s3.entry.as_ref().unwrap().phase,NativeEntryPhase::SpotExposed);
        let first=audit_okx_first_leg_before_hedge(
          &api,&native,s3.generation,&creds,now,OkxDomain::Global,1).unwrap();
        assert!(first.is_complete_readonly_candidate);
        let (s4,_)=native.record_native_spot_first_leg_evidence(first).unwrap();
        let bills=audit_okx_first_leg_cash_bills_window(
          &api,&native,s4.generation,&creds,now,OkxDomain::Global,1).unwrap();
        assert!(bills.window_matched);
        let (s5,_)=native.record_native_spot_prehedge_bills_evidence(bills).unwrap();
        let (_s6,futures_permit)=native.prepare_futures_entry_hedge(
          s5.generation,7,"r138-future-intent","r138-future-client").unwrap();
        let unknown=native.record_futures_entry_observation(
          futures_permit,0,None,false,false,true).unwrap();
        assert_eq!(unknown.entry.as_ref().unwrap().phase,
          NativeEntryPhase::RecoveryRequired);
        let query=native.pending_native_entry_query(unknown.generation).unwrap();
        assert_eq!(query.leg,NativeEntryLeg::Futures);
        assert_eq!(query.client_id,"r138-future-client");
        let baseline=spotarb_native_market_core::
          native_okx_persisted_fee_census_r211_review::
          inspect_persisted_okx_fee_directory_r211(
             &config,now,"research-owner",&archive_dir,&inputs,&[]).unwrap();
        let joined=join(&config,now,"research-owner",&archive_dir,
           &inputs,&native,unknown.generation,&[]).unwrap();
        assert_eq!(joined.native_spot_pending,0);
        assert_eq!(joined.native_futures_pending,1);
        assert_eq!(joined.local_unknown_count,0);
        assert_eq!(joined.directory,baseline);
        assert_eq!(joined.directory.local_archives_checked,1);
        assert!(!joined.may_trade_real);
        assert!(!joined.complete_account_fees_proven);
        assert!(!joined.shared_exchange_account_proven);
        let cold=NativeStateStore::open(folder.join("r138-native.json")).unwrap();
        assert_eq!(join(&config,now,"research-owner",&archive_dir,
          &inputs,&cold,unknown.generation,&[]).unwrap(),joined);
        assert!(join(&config,now,"research-owner",&archive_dir,
          &inputs,&cold,unknown.generation+1,&[]).is_err());
        let mut altered=config.clone();
        altered.raw["real_trial"]["deny_on_unknown"]=serde_yaml::Value::Bool(false);
        assert!(join(&altered,now,"research-owner",&archive_dir,
          &inputs,&cold,unknown.generation,&[]).is_err());
        let native_before=fs::read(folder.join("r138-native.json")).unwrap();
        let archive_before=fs::read(&archive).unwrap();
        let extended=join(&config,now,"research-owner",&archive_dir,
          &inputs,&cold,unknown.generation,
          &["client:OTHER_OKX".to_string()]).unwrap();
        assert_eq!(extended.local_unknown_count,1);
        assert_eq!(extended.native_futures_pending,1);
        assert_eq!(fs::read(folder.join("r138-native.json")).unwrap(),native_before);
        assert_eq!(fs::read(&archive).unwrap(),archive_before);
        let extra=archive_dir.join("omitted.json");
        fs::copy(&archive,&extra).unwrap();
        assert!(join(&config,now,"research-owner",&archive_dir,
          &inputs,&cold,unknown.generation,&[]).is_err());
        fs::remove_dir_all(folder).unwrap();
        // Original C6 mock and R211 owner remain review-only.
     }
     #[test]
     fn r136_cold_native_pending_is_not_omitted_from_r211(){
        use spotarb_native_market_core::{
          authoritative_state::NativeAuthoritativeState,
          fill_economics::{PositionEconomicsState,EntryEconomicsState,
            EntryTiming,LegFillAggregate},
          native_okx_persisted_fee_census_r211_review::
            inspect_native_pending_directory_r136 as join,
        };
        let (folder,store,owner_file)=setup(Kind::ExactTargetShort);
        let original=read(&store);
        let dir=folder.join("r136-archives");
        fs::create_dir(&dir).unwrap();
        let archive=dir.join("one.json");
        use spotarb_native_market_core::native_paper_v2_close_source_archive_review::{
          write_local_close_source_once,LocalArchiveFailpoint,
          PersistedLocalArchivedCloseCandidate,
        };
        write_local_close_source_once(&archive,&original,&close(),
          LocalArchiveFailpoint::None).unwrap();
        let inputs=[PersistedLocalArchivedCloseCandidate{
          preclose_v2_file:&owner_file,archive_path:&archive}];
        let cfg=NativeTradingConfig::parse(CONFIG).unwrap();
        let native=NativeStateStore::open(folder.join("r136-native.json")).unwrap();
        native.create(&NativeAuthoritativeState::empty_from_legacy(
          "11".repeat(32),0)).unwrap();
        let qty=a("0.09").unwrap();
        let econ=PositionEconomicsState{entry:EntryEconomicsState{
          signal_gross_microbps:100_000_000,
          signal_expected_net_microbps:80_000_000,
          signal_spot_price_atoms:Some(a("100").unwrap()),
          signal_futures_price_atoms:Some(a("101").unwrap()),
          projected_cost_reserve_microbps:20_000_000,
          spot:LegFillAggregate::new("okx","BNT/USDT","buy",qty).unwrap(),
          futures:LegFillAggregate::new("binance","BNTUSDT","sell",qty).unwrap(),
          timing:EntryTiming::default(),
          post_leg1_projected_gross_microbps:None,
          post_leg1_projected_net_microbps:None,
          locked_actual_gross_microbps:None,
          locked_actual_net_microbps:None,
          signal_to_leg1_slippage_microbps:None,
          signal_to_hedge_decay_microbps:None,
          execution_cost_vs_signal_microbps:None,
        },exit:None};
        let (state,_permit)=native.begin_native_entry(1,
          seeded(true).plan,econ.clone(),"r136-spot-intent",
          "sab00000000000000000000000000136").unwrap();
        let now=1_780_000_006_000;
        let vanilla=spotarb_native_market_core::
          native_okx_persisted_fee_census_r211_review::
          inspect_persisted_okx_fee_directory_r211(
            &cfg,now,"research-owner",&dir,&inputs,&[]).unwrap();
        assert_eq!(vanilla.fee_day.quote_fee_day.open_or_unknown_order_count,0);
        let joined=join(&cfg,now,"research-owner",&dir,
           &inputs,&native,state.generation,&[]).unwrap();
        assert_eq!(joined.native_spot_pending,1);
        assert_eq!(joined.native_futures_pending,0);
        assert_eq!(joined.local_unknown_count,1);
        assert_eq!(joined.directory.local_archives_checked,1);
        assert_eq!(joined.directory.fee_day.quote_fee_day.open_or_unknown_order_count,1);
        assert_eq!(joined.directory.fee_day.quote_fee_day.known_quote_okx_fees_e12,
          vanilla.fee_day.quote_fee_day.known_quote_okx_fees_e12);
        assert!(!joined.may_trade_real&&!joined.shared_exchange_account_proven);
        let reopened=NativeStateStore::open(folder.join("r136-native.json")).unwrap();
        assert_eq!(join(&cfg,now,"research-owner",&dir,
          &inputs,&reopened,state.generation,&[]).unwrap(),joined);
        assert!(join(&cfg,now,"research-owner",&dir,
          &inputs,&reopened,state.generation+1,&[]).is_err());
        let second=join(&cfg,now,"research-owner",&dir,
          &inputs,&reopened,state.generation,
          &["client:ANOTHER".to_string()]).unwrap();
        assert_eq!(second.local_unknown_count,2);
        assert!(!second.may_trade_real);
        let id=reopened.pending_native_entry_query(state.generation)
            .unwrap().client_id;
        assert!(join(&cfg,now,"research-owner",&dir,&inputs,
           &reopened,state.generation,&[format!("client:{id}")]).is_err(),
           "duplicate original native ID must not disappear in deduplication");
        let mut altered=cfg.clone();
        altered.raw["real_trial"]["deny_on_unknown"]=serde_yaml::Value::Bool(false);
        assert!(join(&altered,now,"research-owner",&dir,
          &inputs,&reopened,state.generation,&[]).is_err());
        let omitted=dir.join("missing_archive.json");
        fs::copy(&archive,&omitted).unwrap();
        assert!(join(&cfg,now,"research-owner",&dir,
          &inputs,&reopened,state.generation,&[]).is_err());
        fs::remove_file(&omitted).unwrap();
        let future=NativeStateStore::open(folder.join("r137-futures.json")).unwrap();
        future.create(&NativeAuthoritativeState::empty_from_legacy(
           "22".repeat(32),0)).unwrap();
        let (_s2,spot_permit)=future.begin_native_entry(1,
           seeded(true).plan,econ,"r137-future-spot",
           "sab00000000000000000000000000137").unwrap();
        let s3=future.record_spot_entry_observation(spot_permit,
           seeded(true).plan.planned_qty_atoms_e18,
           Some(a("100").unwrap()),true,false,false).unwrap();
        // Normal-linked Rust must NOT prepare futures from mere cumulative
        // Spot ACK: C6 full fills and account bills are prerequisites.
        let before=fs::read(folder.join("r137-futures.json")).unwrap();
        assert!(future.prepare_futures_entry_hedge(
           s3.generation,7,"r137-futures-intent","r137-futures-client").is_err());
        assert_eq!(fs::read(folder.join("r137-futures.json")).unwrap(),before);
        let futures_cold=NativeStateStore::open(
           folder.join("r137-futures.json")).unwrap();
        assert!(futures_cold.pending_native_entry_query(
           s3.generation).is_err(),"filled Spot is not an unresolved opening");
        assert!(join(&cfg,now,"research-owner",&dir,&inputs,
          &futures_cold,s3.generation,&[]).is_err());
        // Filled Spot without C6 lineage is NOT a valid Futures UNKNOWN.
        fs::remove_dir_all(folder).unwrap();
        // NativeState and R211 remain local; no exchange-wide permit.
     }
     #[test]
     fn r211_omitted_archive_cannot_mint_complete_local_fee_census(){
         use spotarb_native_market_core::{
          native_paper_v2_close_source_archive_review::{
           write_local_close_source_once,LocalArchiveFailpoint,
           LocalArchivedCloseCandidate,PersistedLocalArchivedCloseCandidate,
          },
          native_okx_persisted_fee_census_r211_review::
           inspect_persisted_okx_fee_directory_r211 as strict,
          native_okx_close_fee_archive_r210_review::
           inspect_archived_okx_buy_sell_fee_day_r210 as subset,
         };
         let (folder,store,owner_file)=setup(Kind::ExactTargetShort);
         let owner=read(&store);
         let dir=folder.join("r211_dir");
         fs::create_dir(&dir).unwrap();
         let included=dir.join("one.json");
         let omitted=dir.join("two.json");
         write_local_close_source_once(&included,&owner,&close(),
           LocalArchiveFailpoint::None).unwrap();
         let cfg=NativeTradingConfig::parse(CONFIG).unwrap();
         let strict_rows=[PersistedLocalArchivedCloseCandidate{
           preclose_v2_file:&owner_file,archive_path:&included,
         }];
         assert!(strict(&cfg,1_780_000_006_000,
           "research-owner",&dir,&strict_rows,&[]).is_ok());
         write_local_close_source_once(&omitted,&owner,&close(),
           LocalArchiveFailpoint::None).unwrap();
         let subset_rows=[LocalArchivedCloseCandidate{
           preclose_v2:&owner,archive_path:&included,
         }];
         assert!(subset(&cfg,1_780_000_006_000,
           "research-owner",&subset_rows,&[]).is_ok(),
           "R210 subset scope remains explicitly weaker");
         assert!(strict(&cfg,1_780_000_006_000,
           "research-owner",&dir,&strict_rows,&[]).is_err(),
           "R211 detects omitted local archive");
         fs::remove_file(&omitted).unwrap();
         let unexpected=dir.join("unexpected.txt");
         fs::write(&unexpected,b"not archive").unwrap();
         assert!(strict(&cfg,1_780_000_006_000,
           "research-owner",&dir,&strict_rows,&[]).is_err());
         fs::remove_file(&unexpected).unwrap();
         let mut bytes=fs::read(&owner_file).unwrap();
         let middle=bytes.len()/2;bytes[middle]^=1;
         fs::write(&owner_file,bytes).unwrap();
         assert!(strict(&cfg,1_780_000_006_000,
           "research-owner",&dir,&strict_rows,&[]).is_err(),
           "cold local owner corruption must fail closed");
         fs::remove_dir_all(folder).unwrap();
     }
     #[test]
     fn r210_archived_sell_fee_day_separates_unvalued_base_entry_fee(){
         use spotarb_native_market_core::{
             native_paper_v2_close_source_archive_review::{
                 write_local_close_source_once,LocalArchiveFailpoint,
                 LocalArchivedCloseCandidate,
             },
             native_okx_close_fee_archive_r210_review::
                 inspect_archived_okx_buy_sell_fee_day_r210 as review,
         };
         let (folder,store,_path)=setup(Kind::ExactTargetShort);
         let v2=read(&store);
         let source=close();
         let archive=folder.join("r210_fee_source.json");
         write_local_close_source_once(
             &archive,&v2,&source,LocalArchiveFailpoint::None).unwrap();
         let c=NativeTradingConfig::parse(CONFIG).unwrap();
         let candidate=[LocalArchivedCloseCandidate{
             preclose_v2:&v2,archive_path:&archive,
         }];
         let view=review(&c,1_780_000_006_000,
            "research-owner",&candidate,&[]).unwrap();
         assert_eq!(view.claimed_local_closed_cycles,1);
         assert_eq!(view.included_buy_quote_fee_fills,0);
         assert_eq!(view.included_sell_quote_fee_fills,1);
         assert_eq!(view.unvalued_buy_base_fee_fills,1);
         assert_eq!(view.unvalued_buy_base_fee_atoms_e12,
             a("0.001").unwrap());
         assert_eq!(view.unvalued_today_buy_base_fee_fills,1);
         assert_eq!(view.unvalued_today_buy_base_fee_atoms_e12,a("0.001").unwrap());
         assert_eq!(view.quote_fee_day.known_quote_okx_fees_e12,
             a("0.009").unwrap());
         assert!(!view.complete_quote_and_base_fee_valuation_proven);
         assert!(!view.quote_fee_day.below_cap_authoritatively_proven);
         assert!(!view.exchange_account_authenticated);
         assert!(!view.independently_verified_closed);
         assert!(!view.may_trade_real);
         // Unvalued native-asset fees are also attributed to the
         // execution day; never silently assign them to exit day.
         let next=(spotarb_native_market_core::native_daily_risk::yekt_day_index(
              1_780_000_006_000).unwrap()+1)
              *spotarb_native_market_core::native_daily_risk::UTC_DAY_MS
              -spotarb_native_market_core::native_daily_risk::YEKT_OFFSET_MS+1;
         let rolled=review(&c,next,"research-owner",&candidate,&[]).unwrap();
         assert_eq!(rolled.quote_fee_day.known_today_fill_count,0);
         assert_eq!(rolled.quote_fee_day.known_earlier_fill_count,1);
         assert_eq!(rolled.unvalued_today_buy_base_fee_fills,0);
         assert_eq!(rolled.unvalued_today_buy_base_fee_atoms_e12,0);
         assert_eq!(rolled.unvalued_buy_base_fee_atoms_e12,a("0.001").unwrap());
         fs::remove_dir_all(folder).unwrap();
     }
     #[test]
     fn r210_duplicate_local_owner_archive_and_bad_config_denied(){
         use spotarb_native_market_core::{
             native_paper_v2_close_source_archive_review::{
                 write_local_close_source_once,LocalArchiveFailpoint,
                 LocalArchivedCloseCandidate,
             },
             native_okx_close_fee_archive_r210_review::
                 inspect_archived_okx_buy_sell_fee_day_r210 as review,
         };
         let (folder,store,_path)=setup(Kind::ExactTargetShort);
         let owner=read(&store);
         let close_row=close();
         let a_path=folder.join("r210_original_archive_a.json");
         let b_path=folder.join("r210_original_archive_b.json");
         for path in [&a_path,&b_path]{
             write_local_close_source_once(
                 path,&owner,&close_row,LocalArchiveFailpoint::None).unwrap();
         }
         let input=[
             LocalArchivedCloseCandidate{preclose_v2:&owner,archive_path:&a_path},
             LocalArchivedCloseCandidate{preclose_v2:&owner,archive_path:&b_path},
         ];
         let config=NativeTradingConfig::parse(CONFIG).unwrap();
         assert!(review(&config,1_780_000_006_000,
             "research-owner",&input,&[]).is_err(),
             "one V2 owner cannot charge the same SELL fee twice");
         let only=[LocalArchivedCloseCandidate{
             preclose_v2:&owner,archive_path:&a_path}];
         let mut mutated=config.clone();
         mutated.policy.daily_okx_fee_cap_e18=1;
         assert!(review(&mutated,1_780_000_006_000,
             "research-owner",&only,&[]).is_err());
         // The local archive was write-once, but a changed byte must
         // still fail the existing source archive integrity reader.
         let mut bytes=fs::read(&a_path).unwrap();
         bytes[6]^=1;fs::write(&a_path,bytes).unwrap();
         assert!(review(&config,1_780_000_006_000,
             "research-owner",&only,&[]).is_err());
         fs::remove_dir_all(folder).unwrap();
     }
     #[test]
     fn local_four_leg_close_source_archive_fsync_cold_recovery_is_single_use(){
         use spotarb_native_market_core::{
             native_paper_v2_close_source_archive_review::{
                 write_local_close_source_once,read_local_close_source,LocalArchiveFailpoint,
                 inspect_supplied_local_archived_close_census,
                 inspect_complete_local_archive_directory_census,
                 inspect_persisted_local_archive_directory_census,
                 LocalArchivedCloseCandidate,PersistedLocalArchivedCloseCandidate,
             },
             native_trade2f3_v2_versioned_store::V2ReviewPhase,
         };
         let (folder,store,owner_file)=setup(Kind::ExactTargetShort);
         let v2=read(&store);
         let source=close();
         let file=folder.join("source_close_once.json");
         let digest=write_local_close_source_once(
             &file,&v2,&source,LocalArchiveFailpoint::None).unwrap();
         assert_eq!(digest.len(),64);
         assert_eq!(read_local_close_source(&file,&v2).unwrap(),source);
         let single=[LocalArchivedCloseCandidate{
             preclose_v2:&v2,archive_path:&file,
         }];
         let census=inspect_supplied_local_archived_close_census(&single).unwrap();
         assert_eq!(census.claimed_owner_generations,
             vec![v2.genesis.original_owner_generation]);
         assert_eq!(census.local_archives_checked,1);
         assert!(!census.exchange_account_authenticated);
         assert!(!census.complete_owner_universe_proven);
         assert!(!census.independently_verified_closed);
         assert!(!census.may_trade_real);
         assert!(inspect_supplied_local_archived_close_census(&[]).is_err());
         let same_path_twice=[
             LocalArchivedCloseCandidate{preclose_v2:&v2,archive_path:&file},
             LocalArchivedCloseCandidate{preclose_v2:&v2,archive_path:&file},
         ];
         assert!(inspect_supplied_local_archived_close_census(&same_path_twice).is_err());
         // A second independently named archive of the SAME local owner
         // must not mint another closed cycle in the proposed census.
         let duplicate_file=folder.join("source_same_owner_second_file.json");
         write_local_close_source_once(
             &duplicate_file,&v2,&source,LocalArchiveFailpoint::None).unwrap();
         let same_owner_twice=[
             LocalArchivedCloseCandidate{preclose_v2:&v2,archive_path:&file},
             LocalArchivedCloseCandidate{preclose_v2:&v2,archive_path:&duplicate_file},
         ];
         assert!(inspect_supplied_local_archived_close_census(&same_owner_twice).is_err());
         // An explicit archive-only directory census MUST see archives
         // missing from the caller's nominated list, unlike a subset read.
         let archive_dir=folder.join("dedicated_archive_only");
         std::fs::create_dir(&archive_dir).unwrap();
         let first_dir_archive=archive_dir.join("first.json");
         write_local_close_source_once(
             &first_dir_archive,&v2,&source,LocalArchiveFailpoint::None).unwrap();
         let directory_first=[LocalArchivedCloseCandidate{
             preclose_v2:&v2,archive_path:&first_dir_archive,
         }];
         let one=inspect_complete_local_archive_directory_census(
             &archive_dir,&directory_first).unwrap();
         assert_eq!(one.local_archives_checked,1);
         assert!(!one.complete_owner_universe_proven);
         assert!(!one.exchange_account_authenticated);
         assert!(!one.independently_verified_closed);
         assert!(!one.may_trade_real);
         let second_dir_archive=archive_dir.join("second.json");
         write_local_close_source_once(
             &second_dir_archive,&v2,&source,LocalArchiveFailpoint::None).unwrap();
         // The old caller-supplied subset check still passes, proving
         // the newly added directory scan closes a LOCAL omission gap.
         assert!(inspect_supplied_local_archived_close_census(
             &directory_first).is_ok());
         assert!(inspect_complete_local_archive_directory_census(
             &archive_dir,&directory_first).is_err());
         let directory_both=[
             LocalArchivedCloseCandidate{
                 preclose_v2:&v2,archive_path:&first_dir_archive},
             LocalArchivedCloseCandidate{
                 preclose_v2:&v2,archive_path:&second_dir_archive},
         ];
         assert!(inspect_complete_local_archive_directory_census(
             &archive_dir,&directory_both).is_err(),
             "two files for one owner cannot create two closes");
         std::fs::remove_file(&second_dir_archive).unwrap();
         let foreign=[LocalArchivedCloseCandidate{
             preclose_v2:&v2,archive_path:&file,
         }];
         assert!(inspect_complete_local_archive_directory_census(
             &archive_dir,&foreign).is_err());
         std::fs::write(archive_dir.join("unexpected.txt"),b"not an archive").unwrap();
         assert!(inspect_complete_local_archive_directory_census(
             &archive_dir,&directory_first).is_err());
         std::fs::remove_file(archive_dir.join("unexpected.txt")).unwrap();
         #[cfg(unix)]
         {
             let alias=archive_dir.join("symlink.json");
             std::os::unix::fs::symlink(&first_dir_archive,&alias).unwrap();
             assert!(inspect_complete_local_archive_directory_census(
                 &archive_dir,&directory_first).is_err());
             std::fs::remove_file(alias).unwrap();
         }
         assert!(inspect_complete_local_archive_directory_census(
             &archive_dir,&directory_first).is_ok());
         let cold_source=[PersistedLocalArchivedCloseCandidate{
             preclose_v2_file:&owner_file,archive_path:&first_dir_archive,
         }];
         let cold_census=inspect_persisted_local_archive_directory_census(
             &archive_dir,&cold_source).unwrap();
         assert_eq!(cold_census.claimed_owner_generations,
             vec![v2.genesis.original_owner_generation]);
         assert!(!cold_census.exchange_account_authenticated);
         assert!(!cold_census.complete_owner_universe_proven);
         assert!(!cold_census.independently_verified_closed);
         assert!(!cold_census.may_trade_real);
         #[cfg(unix)]
         {
             let aliased_owner=folder.join("owner_alias.json");
             std::os::unix::fs::symlink(&owner_file,&aliased_owner).unwrap();
             let alias_source=[PersistedLocalArchivedCloseCandidate{
                 preclose_v2_file:&aliased_owner,archive_path:&first_dir_archive,
             }];
             assert!(inspect_persisted_local_archive_directory_census(
                 &archive_dir,&alias_source).is_err());
             std::fs::remove_file(aliased_owner).unwrap();
         }
         assert!(write_local_close_source_once(
             &file,&v2,&source,LocalArchiveFailpoint::None).is_err(),
             "an already persisted source must never be replaced or duplicated");
         // Corrupt bytes in place, then verify a brand-new cold file reader
         // refuses to treat it as a valid local evidence commit.
         let original=std::fs::read(&file).unwrap();
         let mut corrupted=original.clone();
         let flip=corrupted.len()/2;
         corrupted[flip]^=1;
         std::fs::write(&file,corrupted).unwrap();
         assert!(read_local_close_source(&file,&v2).is_err());
         std::fs::write(&file,&original).unwrap();
         assert_eq!(read_local_close_source(&file,&v2).unwrap(),source);
         // An error after file fsync may have already durably written it;
         // recover the exact file, never try to overwrite or fabricate ACK.
         let ambiguous=folder.join("ambiguous_close.json");
         assert!(write_local_close_source_once(
             &ambiguous,&v2,&source,
             LocalArchiveFailpoint::AfterFileSyncBeforeDirectorySync).is_err());
         assert_eq!(read_local_close_source(&ambiguous,&v2).unwrap(),source);
         assert!(write_local_close_source_once(
             &ambiguous,&v2,&source,LocalArchiveFailpoint::None).is_err());
         // A mutable close quantity cannot be re-archived under this
         // V2 revision with the original fee/bill ancestors.
         let mut wrong=source.clone();
         wrong.futures_cover_rows[0].base_qty_atoms_e12-=1;
         assert!(write_local_close_source_once(
             &folder.join("wrong_close.json"),&v2,&wrong,
             LocalArchiveFailpoint::None).is_err());
         // The persisted snapshot is bound to original revision 4.
         store.advance_v2_review(4,V2ReviewPhase::RecoveryOnly,FP::None).unwrap();
         assert!(read_local_close_source(&file,&read(&store)).is_err());
         // Original in-memory source still looks valid against its archive,
         // whereas the actual on-disk owner advanced to recovery revision.
         assert!(inspect_complete_local_archive_directory_census(
             &archive_dir,&directory_first).is_ok());
         assert!(inspect_persisted_local_archive_directory_census(
             &archive_dir,&cold_source).is_err(),
             "stale in-memory owner must not substitute the persisted V2 revision");
         std::fs::remove_dir_all(folder).unwrap();
     }
     // Invoked only by a child PID through --exact. No exchange network.
     #[test]
     fn archived_close_subprocess_worker(){
         use spotarb_native_market_core::{
             native_paper_owner_transaction_review::PaperOwner,
             native_paper_partial_recovery_gate::PaperPartialGate,
             native_paper_portfolio_review::PaperPortfolio,
             native_paper_v2_close_source_archive_review::{
                 write_local_close_source_once,LocalArchiveFailpoint,
             },
             native_paper_v2_cold_files_review::inspect_cold_p002_archived_four_leg_files,
         };
         use std::{collections::BTreeMap,path::PathBuf};
         let Ok(role)=std::env::var("ASTRA_P002_CHILD_ROLE") else{return};
         let dir=PathBuf::from(std::env::var("ASTRA_P002_CHILD_DIR").unwrap());
         let store=NativeStateStore::open(&dir.join("owner.json")).unwrap();
         let paper_file=dir.join("paper.jsonl");
         let archive=dir.join("source_close.json");
         match role.as_str(){
           "archive"=>{
               let v=read(&store);
               write_local_close_source_once(&archive,&v,&close(),
                   LocalArchiveFailpoint::None).unwrap();
               std::process::exit(78);
           }
           "advance"=>{
               assert_eq!(store.advance_v2_review(
                   4,Phase::RecoveryOnly,FP::None).unwrap().revision,5);
               std::process::exit(79);
           }
           "cas"|"compete"|"stale_cas"=>{
               let bytes=std::fs::read(dir.join("seed.json")).unwrap();
               let (portfolio,gates,seq):(
                   PaperPortfolio,BTreeMap<String,PaperPartialGate>,u64
               )=serde_json::from_slice(&bytes).unwrap();
               let mut paper=PaperOwner{
                   portfolio,gate_by_base:gates,journal:paper_file,
                   last_seq:seq,frozen:false,
               };
               let result=store.attach_paper_v2_archived_alignment_review(
                   &mut paper,"BNT",seq,&archive);
               if role=="stale_cas"{
                   match result{
                       Err(_)=>std::process::exit(80),
                       Ok(_)=>std::process::exit(81),
                   }
               }
               if role=="compete"{
                   match result{
                       Ok(committed) if committed==seq+1=>std::process::exit(0),
                       Err(_)=>std::process::exit(11),
                       _=>std::process::exit(12),
                   }
               }
               assert_eq!(result.unwrap(),seq+1);
               std::process::exit(77);
           }
           "cold"=>{
               let seq=std::env::var("ASTRA_P002_COLD_SEQ").unwrap()
                   .parse::<u64>().unwrap();
               let hash=std::env::var("ASTRA_P002_COLD_HASH").unwrap();
               let proof=inspect_cold_p002_archived_four_leg_files(
                   &paper_file,&dir.join("owner.json"),
                   &archive,"BNT",seq,&hash).unwrap();
               assert!(proof.source_archive_verified);
               assert!(!proof.verified_closed&&!proof.may_trade_or_cancel_real);
               assert!(!proof.exchange_account_truth_authenticated);
               std::process::exit(76);
           }
           _=>panic!("unknown child role"),
         }
     }
     #[test]
     fn archived_close_v2_revision_change_in_separate_pid_denies_stale_cas(){
         use spotarb_native_market_core::{
             native_paper_owner_transaction_review::PaperOwner,
             native_paper_v2_close_source_archive_review::{
                 write_local_close_source_once,LocalArchiveFailpoint,
             },
             native_paper_v2_cold_files_review::inspect_cold_p002_archived_four_leg_files,
         };
         use std::process::{Command,Stdio};
         let (dir,store,v2_path)=setup(Kind::ExactTargetShort);
         let original=read(&store);
         let gate=build_v2_compatible_paper(&original,&close(),false);
         let paper=paper_owner_with_gate(&dir,gate);
         let seq=paper.last_seq;
         let paper_path=paper.journal.clone();
         std::fs::write(dir.join("seed.json"),serde_json::to_vec(&(
             &paper.portfolio,&paper.gate_by_base,seq)).unwrap()).unwrap();
         let archive=dir.join("source_close.json");
         write_local_close_source_once(&archive,&original,&close(),
             LocalArchiveFailpoint::None).unwrap();
         let child=|role:&str|{
             Command::new(std::env::current_exe().unwrap())
                 .arg("--exact")
                 .arg("v2_four_leg_native_close_tests::archived_close_subprocess_worker")
                 .env("ASTRA_P002_CHILD_ROLE",role)
                 .env("ASTRA_P002_CHILD_DIR",&dir)
                 .stdout(Stdio::null()).stderr(Stdio::null())
                 .status().unwrap().code()
         };
         assert_eq!(child("advance"),Some(79));
         assert_eq!(child("stale_cas"),Some(80));
         let recovered=PaperOwner::inspect_recovery(&paper_path).unwrap().unwrap();
         assert_eq!(recovered.sequence,seq);
         assert!(inspect_cold_p002_archived_four_leg_files(
             &paper_path,&v2_path,&archive,"BNT",seq,&recovered.hash).is_err());
         assert!(store.attach_paper_v2_archived_alignment_review(
             &mut PaperOwner{
                 portfolio:paper.portfolio,
                 gate_by_base:paper.gate_by_base,
                 journal:paper_path.clone(),last_seq:seq,frozen:false,
             },"BNT",seq,&archive).is_err());
         assert_eq!(PaperOwner::inspect_recovery(&paper_path)
             .unwrap().unwrap().sequence,seq);
         std::fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn archived_close_two_competing_pids_have_one_cas_winner(){
         use spotarb_native_market_core::{
             native_paper_owner_transaction_review::PaperOwner,
             native_paper_v2_close_source_archive_review::{
                 write_local_close_source_once,LocalArchiveFailpoint,
             },
             native_paper_v2_cold_files_review::inspect_cold_p002_archived_four_leg_files,
         };
         use std::process::{Command,Stdio};
         let (dir,store,v2_path)=setup(Kind::ExactTargetShort);
         let v2=read(&store);
         let gate=build_v2_compatible_paper(&v2,&close(),false);
         let owner=paper_owner_with_gate(&dir,gate);
         let seq=owner.last_seq;
         let paper_path=owner.journal.clone();
         let archive=dir.join("source_close.json");
         std::fs::write(dir.join("seed.json"),serde_json::to_vec(&(
              &owner.portfolio,&owner.gate_by_base,seq)).unwrap()).unwrap();
         write_local_close_source_once(&archive,&v2,&close(),
             LocalArchiveFailpoint::None).unwrap();
         let spawn=||{
             Command::new(std::env::current_exe().unwrap())
               .arg("--exact")
               .arg("v2_four_leg_native_close_tests::archived_close_subprocess_worker")
               .env("ASTRA_P002_CHILD_ROLE","compete")
               .env("ASTRA_P002_CHILD_DIR",&dir)
               .stdout(Stdio::null()).stderr(Stdio::null())
               .spawn().unwrap()
         };
         let a=spawn();
         let b=spawn();
         let x=a.wait_with_output().unwrap().status.code();
         let y=b.wait_with_output().unwrap().status.code();
         assert!(
             (x==Some(0)&&y==Some(11))||(x==Some(11)&&y==Some(0)),
             "two contenders must have exactly one durable CAS winner: {x:?} {y:?}");
         let recovered=PaperOwner::inspect_recovery(&paper_path).unwrap().unwrap();
         assert_eq!(recovered.sequence,seq+1);
         let result=inspect_cold_p002_archived_four_leg_files(
             &paper_path,&v2_path,&archive,"BNT",seq+1,&recovered.hash).unwrap();
         assert!(result.source_archive_verified&&!result.may_trade_or_cancel_real);
         std::fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn archived_close_separate_pids_lost_ack_and_exact_cold_recovery(){
         use spotarb_native_market_core::{
             native_paper_owner_transaction_review::PaperOwner,
             native_paper_v2_cold_files_review::inspect_cold_p002_archived_four_leg_files,
             native_paper_v2_close_source_archive_review::{
                 write_local_close_source_once,LocalArchiveFailpoint,
             },
         };
         use std::process::{Command,Stdio};
         let (dir,store,v2_path)=setup(Kind::ExactTargetShort);
         let v2=read(&store);
         let gate=build_v2_compatible_paper(&v2,&close(),false);
         let mut original=paper_owner_with_gate(&dir,gate);
         let parent=original.last_seq;
         let paper_path=original.journal.clone();
         let archive=dir.join("source_close.json");
         let seed=serde_json::to_vec(&(
             &original.portfolio,&original.gate_by_base,parent)).unwrap();
         std::fs::write(dir.join("seed.json"),seed).unwrap();
         let child=|role:&str,seq:u64,hash:&str|{
             Command::new(std::env::current_exe().unwrap())
                 .arg("--exact")
                 .arg("v2_four_leg_native_close_tests::archived_close_subprocess_worker")
                 .arg("--nocapture")
                 .env("ASTRA_P002_CHILD_ROLE",role)
                 .env("ASTRA_P002_CHILD_DIR",&dir)
                 .env("ASTRA_P002_COLD_SEQ",seq.to_string())
                 .env("ASTRA_P002_COLD_HASH",hash)
                 .stdout(Stdio::null()).stderr(Stdio::null())
                 .status().unwrap().code()
         };
         assert_eq!(child("archive",parent,""),Some(78));
         assert!(write_local_close_source_once(
             &archive,&v2,&close(),LocalArchiveFailpoint::None).is_err());
         let first=PaperOwner::inspect_recovery(&paper_path).unwrap().unwrap();
         assert_eq!(first.sequence,parent);
         assert!(inspect_cold_p002_archived_four_leg_files(
             &paper_path,&v2_path,&archive,"BNT",parent,&first.hash).is_err());
         assert_eq!(child("cas",parent,""),Some(77));
         let committed=PaperOwner::inspect_recovery(&paper_path).unwrap().unwrap();
         assert_eq!(committed.sequence,parent+1);
         assert_eq!(child("cold",committed.sequence,&committed.hash),Some(76));
         // In-RAM parent had no ACK, but a durable CAS exists; never
         // create a second local marker through a stale retry.
         assert!(store.attach_paper_v2_archived_alignment_review(
             &mut original,"BNT",parent,&archive).is_err());
         let latest=PaperOwner::inspect_recovery(&paper_path).unwrap().unwrap();
         assert_eq!(latest.sequence,committed.sequence);
         assert_eq!(latest.hash,committed.hash);
         std::fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn archived_four_leg_close_source_and_paper_cas_cold_restart_crash_windows(){
         use spotarb_native_market_core::{
             native_paper_owner_transaction_review::PaperOwner,
             native_paper_v2_close_source_archive_review::{
                 write_local_close_source_once,LocalArchiveFailpoint,
             },
             native_paper_v2_cold_files_review::{
                 inspect_cold_p002_archived_four_leg_files,
             },
             native_trade2f3_v2_versioned_store::V2ReviewPhase,
         };
         for failpoint in [
             LocalArchiveFailpoint::None,
             LocalArchiveFailpoint::AfterFileSyncBeforeDirectorySync
         ]{
             let (folder,store,v2_path)=setup(Kind::ExactTargetShort);
             let v2=read(&store);
             let close_obs=close();
             let gate=build_v2_compatible_paper(&v2,&close_obs,false);
             let mut owner=paper_owner_with_gate(&folder,gate);
             let parent=owner.last_seq;
             let paper_file=owner.journal.clone();
             let archive=folder.join("immutable_close_archive.json");
             let write=write_local_close_source_once(
                 &archive,&v2,&close_obs,failpoint);
             if failpoint==LocalArchiveFailpoint::None{
                 assert!(write.is_ok());
             }else{
                 assert!(write.is_err(),"post-fsync false negative is intentionally ambiguous");
             }
             let inspect=|seq:u64,hash:&str|inspect_cold_p002_archived_four_leg_files(
                 &paper_file,&v2_path,&archive,"BNT",seq,hash);
             let before=PaperOwner::inspect_recovery(&paper_file)
                 .unwrap().unwrap();
             assert_eq!(before.sequence,parent);
             assert!(inspect(parent,&before.hash).is_err(),
                "source archive alone is not a committed durable four-leg CAS");
             let recovered_close=spotarb_native_market_core::
                 native_paper_v2_close_source_archive_review::
                   read_local_close_source(&archive,&v2).unwrap();
             assert_eq!(recovered_close,close_obs);
             assert_eq!(store.attach_paper_v2_archived_alignment_review(
                 &mut owner,"BNT",parent,&archive).unwrap(),parent+1);
             let after=PaperOwner::inspect_recovery(&paper_file)
                 .unwrap().unwrap();
             let proven=inspect(after.sequence,&after.hash).unwrap();
             assert!(proven.source_archive_verified);
             assert!(proven.source_matches_local_paper_cas);
             assert!(proven.local_paper_legs_fee_and_qty_consistent);
             assert!(!proven.exchange_account_truth_authenticated);
             assert!(!proven.external_floor_authenticated_and_fresh);
             assert!(!proven.verified_closed);
             assert!(!proven.may_resume_or_retry_paper);
             assert!(!proven.may_trade_or_cancel_real);
             // A repeated source write and repeated CAS from the old
             // parent revision must each refuse to mint a second identity.
             assert!(write_local_close_source_once(
                 &archive,&v2,&close_obs,LocalArchiveFailpoint::None).is_err());
             assert!(store.attach_paper_v2_archived_alignment_review(
                 &mut owner,"BNT",parent,&archive).is_err());
             assert_eq!(PaperOwner::inspect_recovery(&paper_file)
                 .unwrap().unwrap().sequence,after.sequence);
             assert!(inspect(parent,&after.hash).is_err());
             assert!(inspect(after.sequence,&"0".repeat(64)).is_err());
             let raw=folder.join("only_raw.json");
             std::fs::write(&raw,serde_json::to_vec(&close_obs).unwrap()).unwrap();
             assert!(inspect_cold_p002_archived_four_leg_files(
                 &paper_file,&v2_path,&raw,"BNT",after.sequence,&after.hash).is_err(),
                 "raw fixture cannot impersonate fsynced source envelope");
             let alias=folder.join("archive_alias.json");
             std::os::unix::fs::symlink(&archive,&alias).unwrap();
             assert!(inspect_cold_p002_archived_four_leg_files(
                 &paper_file,&v2_path,&alias,"BNT",after.sequence,&after.hash).is_err());
             let good_bytes=std::fs::read(&archive).unwrap();
             let mut broken=good_bytes.clone();
             broken[18]^=1;
             std::fs::write(&archive,&broken).unwrap();
             assert!(inspect(after.sequence,&after.hash).is_err());
             std::fs::write(&archive,&good_bytes).unwrap();
             assert_eq!(inspect(after.sequence,&after.hash).unwrap(),proven);
             store.advance_v2_review(4,V2ReviewPhase::RecoveryOnly,FP::None).unwrap();
             assert!(inspect(after.sequence,&after.hash).is_err(),
                "V2 owner revision drift disallows archive re-binding");
             std::fs::remove_dir_all(folder).unwrap();
         }
     }
     #[test]
     fn cold_disk_p0_02_sources_match_exact_mock_four_legs_but_never_authorize(){
         use spotarb_native_market_core::{
             native_paper_owner_transaction_review::PaperOwner,
             native_paper_v2_cold_files_review::
                 inspect_cold_p002_four_leg_files,
         };
         let (folder,store,v2_path)=setup(Kind::ExactTargetShort);
         let v2=read(&store);
         let close_observation=close();
         let original_gate=build_v2_compatible_paper(&v2,&close_observation,false);
         let mut paper=paper_owner_with_gate(&folder,original_gate);
         let before=paper.last_seq;
         assert_eq!(store.attach_paper_v2_alignment_review(
             &mut paper,"BNT",before,&close_observation).unwrap(),before+1);
         let paper_path=paper.journal.clone();
         let cold=PaperOwner::inspect_recovery(&paper_path).unwrap().unwrap();
         let obs_path=folder.join("closing_source.json");
         let initial_bytes=serde_json::to_vec(&close_observation).unwrap();
         std::fs::write(&obs_path,&initial_bytes).unwrap();
         let test=|seq:u64,floor:&str|inspect_cold_p002_four_leg_files(
             &paper_path,&v2_path,&obs_path,"BNT",seq,floor);
         let accepted=test(cold.sequence,&cold.hash).unwrap();
         assert_eq!(accepted.paper_sequence,before+1);
         assert_eq!(accepted.source_v2_revision,4);
         assert!(accepted.source_matches_local_paper_cas);
         assert!(accepted.local_paper_legs_fee_and_qty_consistent);
         assert!(!accepted.exchange_account_truth_authenticated);
         assert!(!accepted.external_floor_authenticated_and_fresh);
         assert!(!accepted.verified_closed);
         assert!(!accepted.may_resume_or_retry_paper);
         assert!(!accepted.may_trade_or_cancel_real);
         // A stale or forged paper floor denies; the local file itself is not
         // sufficient to claim an independently authenticated latest state.
         assert!(test(before,&cold.hash).is_err());
         assert!(test(cold.sequence,&"0".repeat(64)).is_err());
         // Changing even one source-shaped closing amount is not reinterpreted
         // as a completed hedge/flat account state.
         let mut corrupt=close_observation.clone();
         corrupt.spot_sell_rows[0].base_qty_atoms_e12-=1;
         std::fs::write(&obs_path,serde_json::to_vec(&corrupt).unwrap()).unwrap();
         assert!(test(cold.sequence,&cold.hash).is_err());
         // Restored correct source must match exact durable local marker.
         std::fs::write(&obs_path,&initial_bytes).unwrap();
         assert_eq!(test(cold.sequence,&cold.hash).unwrap(),accepted);
         // The on-disk PAPER CAS row must also retain exact hash, not
         // just a caller-provided floor string.
         let original_paper_bytes=std::fs::read(&paper_path).unwrap();
         let mut bad_paper=original_paper_bytes.clone();
         let middle=bad_paper.len()/2;
         bad_paper[middle]^=1;
         std::fs::write(&paper_path,&bad_paper).unwrap();
         assert!(test(cold.sequence,&cold.hash).is_err());
         std::fs::write(&paper_path,&original_paper_bytes).unwrap();
         assert_eq!(test(cold.sequence,&cold.hash).unwrap(),accepted);
         let paper_link=folder.join("paper_symlink.jsonl");
         std::os::unix::fs::symlink(&paper_path,&paper_link).unwrap();
         assert!(inspect_cold_p002_four_leg_files(
             &paper_link,&v2_path,&obs_path,"BNT",cold.sequence,&cold.hash).is_err());
         // A revision-5 successor is NOT the immutable revision-4 source that
         // was originally bound to the paper journal.
         use spotarb_native_market_core::native_trade2f3_v2_versioned_store::V2ReviewPhase;
         store.advance_v2_review(4,V2ReviewPhase::RecoveryOnly,FP::None).unwrap();
         assert!(test(cold.sequence,&cold.hash).is_err());
         std::fs::remove_dir_all(folder).unwrap();
     }
     // A dedicated isolated child commits R15's terminal quantity, then
     // is killed by SIGKILL before any R14/PAPER successor publication.
     #[test]
     fn p002_r15_terminal_sigkill_child(){
       use spotarb_native_market_core::position_lifecycle::{
         DurableLifecycleStore,LifecycleState,
       };
       let Ok(file)=std::env::var("ASTRA_P002_R15_KILL_FILE") else{return};
       let mode=std::env::var("ASTRA_P002_R15_KILL_MODE").unwrap();
       let store=DurableLifecycleStore::open(&file).unwrap();
       let mut life=store.load().unwrap().unwrap();
       let qty=life.pending.as_ref().unwrap().requested_units;
       assert!(qty>1);
       life.pending.as_mut().unwrap().recovery_terminal_filled_units=
         Some(if mode=="drift"{qty-1}else{qty});
       life.state=LifecycleState::RecoveryRequired;
       store.persist(&life).unwrap(); // returned only after durable write
       let pid=std::process::id().to_string();
       std::process::Command::new("/usr/bin/kill")
         .arg("-9").arg(pid).status().unwrap();
       panic!("SIGKILL was not delivered to child");
     }
     #[test]
     fn p002_r90_seven_store_sigkill_after_r15_fsync_never_retries(){
       use spotarb_native_market_core::{
         execution::Leg,execution_store::DurableMutationGate,
         native_paper_owner_transaction_review::PaperOwner,
         native_paper_v2_close_source_archive_review::{
           write_local_close_source_once,LocalArchiveFailpoint,
         },
         native_r15_original_owner_binding_review::{
           prepare_r15_original_owner_binding,persist_r15_original_owner_binding_once,
         },
         native_r14_origination_claim_review::{
           derive_r14_origination_claim,publish_r14_origination_claim_once,
           inspect_r90_seven_source_cold_claim,inspect_r90_terminal_v2_r15_cold,
           inspect_r90_terminal_v2_at_caller_floor,R90CallerSuppliedFloor,
           inspect_r90_terminal_v2_signed_latest,r90_signed_seven_message,
           R90SignedSevenFloor,R90SignedSevenReply,R90PinnedSevenLatest,
           R90_LATEST_DOMAIN,
         },
         native_paper_durable_journal_review::append_terminal_v2_if_prev,
         position_lifecycle::{
           PositionLifecycle,PendingCloseIntent,CloseLeg,DurableLifecycleStore,
         },
       };
       use std::os::unix::process::ExitStatusExt;
       use std::process::{Command,Stdio};
       for (leg_name,mode) in [("futures","exact"),("futures","drift"),("spot","exact"),("spot","drift")]{
         let (dir,v2_store,v2_file)=setup(Kind::ExactTargetShort);
         let v=read(&v2_store);let obs=close();
         let archive=dir.join("r90_sigkill_source.json");
         write_local_close_source_once(&archive,&v,&obs,
           LocalArchiveFailpoint::None).unwrap();
         let mut paper=paper_owner_with_gate(
           &dir,build_v2_compatible_paper(&v,&obs,false));
         let parent=paper.last_seq;
         v2_store.attach_paper_v2_archived_alignment_review(
           &mut paper,"BNT",parent,&archive).unwrap();
         let recovered=PaperOwner::inspect_recovery(&paper.journal)
           .unwrap().unwrap();
         let target=v.genesis.planned_net_short_atoms_e12;
         let mut life=PositionLifecycle::new_hedged(target,99).unwrap();
         life.start_close().unwrap();
         let (intent_id,client_id,close_leg,r14_leg)=if leg_name=="spot"{
           life.futures_filled_units=target;
           (obs.spot_sell_intent_id.clone(),obs.spot_sell_client_id.clone(),
             CloseLeg::Spot,Leg::Spot)
         }else{
           (obs.futures_cover_intent_id.clone(),obs.futures_cover_client_id.clone(),
             CloseLeg::Futures,Leg::Futures)
         };
         life.pending=Some(PendingCloseIntent{
           intent_id:intent_id.clone(),client_id:client_id.clone(),
           leg:close_leg,requested_units:target,
           recovery_terminal_filled_units:None,
           recovery_original_exchange_order_id:Some(if leg_name=="spot"{
               obs.spot_sell_order_id.clone()
           }else{obs.futures_cover_order_id.clone()}),
         });
         let r15=dir.join("r90_sigkill_r15.json");
         DurableLifecycleStore::open(&r15).unwrap().persist(&life).unwrap();
         let owner=prepare_r15_original_owner_binding(&life,&v).unwrap();
         let sidecar=dir.join("r90_sigkill_owner.json");
         persist_r15_original_owner_binding_once(&sidecar,&owner).unwrap();
         let r14=dir.join("r90_sigkill_r14.json");
         let mut gate=DurableMutationGate::open(&r14).unwrap();
         let intent=intent_id.as_str();
         let client=client_id.as_str();
         gate.begin(intent,r14_leg,client).unwrap();
         gate.acknowledge(intent,false).unwrap();
         let rows=gate.store.load().unwrap();
         let claim=derive_r14_origination_claim(&rows[0],&life,&owner).unwrap();
         let claim_path=dir.join("r90_sigkill_claim.json");
         publish_r14_origination_claim_once(&claim_path,&claim).unwrap();
         let cold=||inspect_r90_seven_source_cold_claim(
           &claim_path,&r14,&r15,&sidecar,&v2_file,&archive,
           &paper.journal,"BNT",recovered.sequence,&recovered.hash);
         let before=cold().unwrap();
         assert!(!before.may_send_retry_or_resume&&!before.verified_closed);
         let status=Command::new(std::env::current_exe().unwrap())
           .arg("--exact")
           .arg("v2_four_leg_native_close_tests::p002_r15_terminal_sigkill_child")
           .env("ASTRA_P002_R15_KILL_FILE",&r15)
           .env("ASTRA_P002_R15_KILL_MODE",mode)
           .stdout(Stdio::null()).stderr(Stdio::null())
           .status().unwrap();
         assert_eq!(status.signal(),Some(9),"child did not die by SIGKILL");
         let restored=DurableLifecycleStore::open(&r15)
           .unwrap().load().unwrap().unwrap();
         assert_eq!(restored.pending.as_ref().unwrap().recovery_terminal_filled_units,
           Some(if mode=="drift"{target-1}else{target}));
         let after=cold();
         if mode=="drift"{
           assert!(after.is_err(),"R15 partial staged terminal must conflict with full V2/PAPER");
         }else{
           let exact=after.unwrap();
           assert!(exact.same_original_local_close_owner);
           assert!(!exact.may_send_retry_or_resume&&!exact.verified_closed);
         }
         // R90 must not treat the earlier v1 source as a durable terminal-v2 seal.
         let v1_only=inspect_r90_terminal_v2_r15_cold(
           &claim_path,&r14,&r15,&sidecar,&v2_file,&archive,
           &paper.journal,"BNT",recovered.sequence,&recovered.hash);
         assert!(v1_only.is_err(),"R90 terminal close cannot consume unsealed PAPER");
         let v2_seq=append_terminal_v2_if_prev(
           &paper.journal,"BNT",recovered.sequence,&v,&obs).unwrap();
         let terminal=PaperOwner::inspect_recovery(&paper.journal).unwrap().unwrap();
         assert_eq!(terminal.sequence,v2_seq);
         assert_eq!(terminal.sealed_terminal_v2.len(),1);
         let terminal_join=||inspect_r90_terminal_v2_r15_cold(
           &claim_path,&r14,&r15,&sidecar,&v2_file,&archive,
           &paper.journal,"BNT",terminal.sequence,&terminal.hash);
         if mode=="drift"{
           assert!(terminal_join().is_err(),
             "R15 staged partial must never consume full terminal V2");
         }else{
           let review=terminal_join().unwrap();
           assert!(review.terminal_v2_matches_staged_r15_close);
           assert!(review.original_local_owner_matches);
           assert!(review.pending_exchange_reconciliation_required);
           assert!(!review.may_send_retry_or_resume&&!review.verified_closed);
           assert!(!review.exchange_authenticated&&!review.cross_store_atomic_proven);
           // A terminal ledger alone cannot fabricate missing R15 fsynced fill.
           DurableLifecycleStore::open(&r15).unwrap().persist(&life).unwrap();
           assert!(terminal_join().is_err(),"terminal R15 missing stage accepted");
           DurableLifecycleStore::open(&r15).unwrap().persist(&restored).unwrap();
           assert!(terminal_join().is_ok());
           // R14 merely Prepared is not proof of a terminal ACK.
           use spotarb_native_market_core::execution::{Intent,IntentState};
           let disk=spotarb_native_market_core::execution_store::
               DurableIntentStore::open(&r14).unwrap();
           disk.persist(&[Intent{id:intent.into(),
             client_order_id:Some(client.into()),state:IntentState::Prepared}]).unwrap();
           assert!(terminal_join().is_err(),"R14 Prepared incorrectly accepted");
           disk.persist(&[Intent{id:intent.into(),
             client_order_id:Some(client.into()),state:IntentState::Acknowledged}]).unwrap();
           assert!(terminal_join().is_ok());
           let source_paths=[&claim_path,&r14,&r15,&sidecar,
             &v2_file,&archive,&paper.journal];
           // Test-only reference held in RAM while exactly ONE local
           // source is rolled back. Not an authenticated external anchor.
           let make_floor=||R90CallerSuppliedFloor{
             schema:1,base:"BNT".into(),paper_sequence:terminal.sequence,
             paper_hash:terminal.hash.clone(),
             seven_file_sha256:std::array::from_fn(|idx|{
               let digest=openssl::sha::sha256(
                 &std::fs::read(source_paths[idx]).unwrap());
               digest.iter().map(|b|format!("{b:02x}")).collect::<String>()
             }),
           };
           let inspect_floor=|floor:&R90CallerSuppliedFloor|
             inspect_r90_terminal_v2_at_caller_floor(
               &claim_path,&r14,&r15,&sidecar,&v2_file,&archive,
               &paper.journal,"BNT",floor);
           let base_floor=make_floor();
           let local=inspect_floor(&base_floor).unwrap();
           assert!(!local.verified_closed&&!local.may_send_retry_or_resume);
           assert!(!local.cross_store_atomic_proven&&!local.exchange_authenticated);
           let original_r14=std::fs::read(&r14).unwrap();
           // A harmless old reconciled intent can coexist with current ACK.
           // This produces a distinct still-valid, internally self-consistent
           // R14 byte revision without changing the original close owner.
           let ack=Intent{id:intent.into(),client_order_id:Some(client.into()),
             state:IntentState::Acknowledged};
           let unrelated=Intent{id:"PREVIOUS_RECONCILED".into(),
             client_order_id:Some("PRIOR_CLIENT".into()),
             state:IntentState::Reconciled};
           disk.persist(&[ack.clone(),unrelated.clone()]).unwrap();
           assert!(terminal_join().is_ok());
           let forward_floor=make_floor();
           assert!(inspect_floor(&forward_floor).is_ok());
           assert!(inspect_floor(&base_floor).is_err(),
             "the older floor must also reject a later byte revision");
           // Simulate rollback of one valid R14 file to its earlier durable
           // revision: the basic local R90 join remains internally happy.
           std::fs::write(&r14,&original_r14).unwrap();
           assert!(terminal_join().is_ok(),
             "control fixture: ordinary local join does not detect benign rollback");
           assert!(inspect_floor(&forward_floor).is_err(),
             "caller-held newer floor must reject a valid older R14");
           // Wrong base or hash cannot be accepted even when source files
           // and PAPER tip are otherwise internally consistent.
           let mut foreign_floor=base_floor.clone();
           foreign_floor.base="FOREIGN".into();
           assert!(inspect_floor(&foreign_floor).is_err());
           let mut altered_floor=base_floor.clone();
           altered_floor.seven_file_sha256[0]="0".repeat(64);
           assert!(inspect_floor(&altered_floor).is_err());
           // Roll forward again, and compare to the saved expected tip.
           disk.persist(&[ack,unrelated]).unwrap();
           assert!(inspect_floor(&forward_floor).is_ok());
           // Other six source digests are pinned just as strictly.
           for (idx,path) in source_paths.iter().enumerate(){
             if idx==1{continue}
             let original=std::fs::read(path).unwrap();
             std::fs::write(path,b"stale source bytes").unwrap();
             assert!(inspect_floor(&forward_floor).is_err(),
               "source index {idx} ignored by supplied floor");
             std::fs::write(path,&original).unwrap();
             assert!(inspect_floor(&forward_floor).is_ok());
           }
           // Two mutually consistent sources now change. A rollback of
           // BOTH R14 and R15 still passes the unsigned local cold join.
           let old_r15=std::fs::read(&r15).unwrap();
           let mut revised_r15=old_r15.clone();
           revised_r15.extend_from_slice(b"\n ");
           std::fs::write(&r15,&revised_r15).unwrap();
           assert!(terminal_join().is_ok());
           let two_source_floor=make_floor();
           let signer=openssl::pkey::PKey::generate_ed25519().unwrap();
           let public=signer.raw_public_key().unwrap().iter()
             .map(|b|format!("{b:02x}")).collect::<String>();
           let nonce="a5".repeat(32);
           let payload=R90SignedSevenFloor{
             domain:R90_LATEST_DOMAIN.into(),
             witness_id:"offline-seven-store-witness".into(),
             nonce_hex:nonce.clone(),witness_revision:41,
             floor:two_source_floor.clone(),
           };
           let signature=openssl::sign::Signer::new_without_digest(&signer)
             .unwrap().sign_oneshot_to_vec(
               &r90_signed_seven_message(&payload).unwrap()).unwrap()
             .iter().map(|b|format!("{b:02x}")).collect::<String>();
           let signed=R90SignedSevenReply{payload,signature_hex:signature};
           let pin=R90PinnedSevenLatest{
             trusted_id:"offline-seven-store-witness".into(),
             public_key_hex:public.clone(),fresh_nonce_hex:nonce.clone(),
             minimum_revision:41,
           };
           let check_signed=|proof:&R90SignedSevenReply,expected:&R90PinnedSevenLatest|
             inspect_r90_terminal_v2_signed_latest(
               &claim_path,&r14,&r15,&sidecar,&v2_file,&archive,
               &paper.journal,"BNT",proof,expected);
           let checked=check_signed(&signed,&pin).unwrap();
           assert!(checked.terminal_v2_matches_staged_r15_close);
           assert!(!checked.may_send_retry_or_resume&&!checked.verified_closed);
           // The signed reply is bound to an independently supplied nonce,
           // witness identity, key and minimum remote revision.
           let mut wrong_pin=pin.clone();
           wrong_pin.fresh_nonce_hex="bb".repeat(32);
           assert!(check_signed(&signed,&wrong_pin).is_err());
           wrong_pin=pin.clone();wrong_pin.trusted_id="other-witness".into();
           assert!(check_signed(&signed,&wrong_pin).is_err());
           wrong_pin=pin.clone();wrong_pin.minimum_revision=42;
           assert!(check_signed(&signed,&wrong_pin).is_err());
           wrong_pin=pin.clone();
           wrong_pin.public_key_hex="00".repeat(32);
           assert!(check_signed(&signed,&wrong_pin).is_err());
           let mut forged=signed.clone();
           forged.payload.floor.seven_file_sha256[1]="0".repeat(64);
           assert!(check_signed(&forged,&pin).is_err());
           forged=signed.clone();
           forged.signature_hex.replace_range(0..2,"ff");
           assert!(check_signed(&forged,&pin).is_err());
           // Simulate coherent directory rollback: R14 & R15 BOTH revert
           // to their earlier valid byte revisions. All local R90 identities
           // and mock original close records are still self-consistent.
           std::fs::write(&r14,&original_r14).unwrap();
           std::fs::write(&r15,&old_r15).unwrap();
           assert!(terminal_join().is_ok(),
             "plain R90 cannot independently detect coherent two-file rollback");
           assert!(check_signed(&signed,&pin).is_err(),
             "signed newer seven-source floor must reject both rolled-back stores");
           // Restore the pair. No automatic repair in the production code.
           disk.persist(&[
             Intent{id:intent.into(),client_order_id:Some(client.into()),
               state:IntentState::Acknowledged},
             Intent{id:"PREVIOUS_RECONCILED".into(),
               client_order_id:Some("PRIOR_CLIENT".into()),
               state:IntentState::Reconciled},
           ]).unwrap();
           std::fs::write(&r15,&revised_r15).unwrap();
           assert!(check_signed(&signed,&pin).is_ok());
           std::fs::write(&r15,&old_r15).unwrap();
           disk.persist(&[Intent{id:intent.into(),
             client_order_id:Some(client.into()),state:IntentState::Acknowledged}])
             .unwrap();
           assert!(terminal_join().is_ok());
         }
         let mut restarted=DurableMutationGate::open(&r14).unwrap();
         assert!(restarted.after_durable_prepare(intent).is_err());
         assert!(restarted.begin("NO_DUPLICATE",r14_leg,"NEW_CID").is_err());
         let still=PaperOwner::inspect_recovery(&paper.journal)
           .unwrap().unwrap();
         assert_eq!(still.sequence,terminal.sequence);
         assert!(!still.may_resume_paper&&!still.may_trade_real);
         fs::remove_dir_all(dir).unwrap();
       }
     }
     #[test]
     fn p002_terminal_v2_same_journal_owner_cas_and_corruption_denied(){
       use spotarb_native_market_core::{
         native_paper_owner_transaction_review::PaperOwner,
         native_paper_durable_journal_review::{
           append_terminal_v2_if_prev,append_snapshot_if_prev,ReviewSnapshot,
         },
       };
       let (dir,store,_)=setup(Kind::ExactTargetShort);
       let v=read(&store);let obs=close();
       let mut paper=paper_owner_with_gate(
         &dir,build_v2_compatible_paper(&v,&obs,false));
       let parent=paper.last_seq;
       // V1 contains closing ledgers but cannot claim durable Closed/PnL.
       let before=PaperOwner::inspect_recovery(&paper.journal).unwrap().unwrap();
       assert_eq!(before.state.closed,0);
       assert!(before.sealed_terminal_v2.is_empty());
       assert!(append_terminal_v2_if_prev(
         &paper.journal,"BNT",parent,&v,&obs).is_err(),
         "V2 terminal cannot be sealed before source alignment");
       store.attach_paper_v2_alignment_review(&mut paper,"BNT",parent,&obs)
          .unwrap();
       let preseal=paper.last_seq;
       let valid_snapshot=ReviewSnapshot::from_paper(
         &paper.portfolio,&paper.gate_by_base).unwrap();
       let mut foreign_obs=obs.clone();
       foreign_obs.futures_cover_order_id="OTHER-BINANCE-COVER".into();
       assert!(append_terminal_v2_if_prev(
         &paper.journal,"BNT",preseal,&v,&foreign_obs).is_err(),
         "foreign terminal original order cannot be durably sealed");
       assert_eq!(PaperOwner::inspect_recovery(&paper.journal)
         .unwrap().unwrap().sequence,preseal);
       let seq=append_terminal_v2_if_prev(
         &paper.journal,"BNT",preseal,&v,&obs).unwrap();
       assert_eq!(seq,preseal+1);
       let cold=PaperOwner::inspect_recovery(&paper.journal).unwrap().unwrap();
       let sealed=&cold.sealed_terminal_v2["BNT"];
       assert_eq!(sealed.original_generation,99);
       assert_eq!(sealed.futures_cover.filled_qty_atoms(),
          v.genesis.planned_net_short_atoms_e12);
       assert_eq!(sealed.spot_sell.filled_qty_atoms(),
          v.genesis.planned_net_short_atoms_e12);
       assert!(sealed.local_source_only);
       assert!(!sealed.verified_closed&&!sealed.may_send_or_retry);
       assert_eq!(cold.state.closed,0);
       assert_eq!(cold.state.modeled_pnl_usdt,0.0);
       assert!(!cold.may_resume_paper&&!cold.may_trade_real);
       assert!(cold.state.positions["BNT"].quarantine);
       assert!(append_terminal_v2_if_prev(
         &paper.journal,"BNT",preseal,&v,&obs).is_err());
       assert!(append_terminal_v2_if_prev(
         &paper.journal,"BNT",seq,&v,&obs).is_err());
       assert!(append_snapshot_if_prev(
         &paper.journal,&valid_snapshot,seq).is_err(),
         "legacy v1 must not bypass a durable V2 terminal owner");
       let original=fs::read(&paper.journal).unwrap();
       let changed=String::from_utf8(original.clone()).unwrap()
         .replacen(r#""local_source_only":true"#,r#""local_source_only":false"#,1);
       assert_ne!(changed.as_bytes(),original.as_slice());
       fs::write(&paper.journal,changed.as_bytes()).unwrap();
       assert!(PaperOwner::inspect_recovery(&paper.journal).is_err());
       fs::write(&paper.journal,&original).unwrap();
       let mut truncated=original.clone();truncated.pop();
       fs::write(&paper.journal,&truncated).unwrap();
       assert!(PaperOwner::inspect_recovery(&paper.journal).is_err());
       fs::write(&paper.journal,&original).unwrap();
       assert_eq!(PaperOwner::inspect_recovery(&paper.journal)
         .unwrap().unwrap().sealed_terminal_v2.len(),1);
       fs::remove_dir_all(dir).unwrap();
     }
     // Separate PID dies after terminal V2 append fsync but before caller ACK.
     #[test]
     fn p002_terminal_v2_sigkill_child(){
       use spotarb_native_market_core::{
         native_paper_durable_journal_review::append_terminal_v2_if_prev,
         native_paper_owner_transaction_review::PaperOwner,
       };
       let Ok(journal)=std::env::var("ASTRA_P002_V2_JOURNAL_KILL") else{return};
       let store_file=std::env::var("ASTRA_P002_V2_OWNER_KILL").unwrap();
       let store=NativeStateStore::open(std::path::Path::new(&store_file)).unwrap();
       let v=read(&store);let obs=close();
       let path=std::path::Path::new(&journal);
       let current=PaperOwner::inspect_recovery(path).unwrap().unwrap();
       append_terminal_v2_if_prev(path,"BNT",current.sequence,&v,&obs).unwrap();
       let status=std::process::Command::new("/usr/bin/kill")
         .arg("-9").arg(std::process::id().to_string()).status().unwrap();
       panic!("SIGKILL child unexpectedly survived: {status:?}");
     }
     #[test]
     fn p002_terminal_v2_recovery_after_sigkill_no_double_seal(){
       use spotarb_native_market_core::{
         native_paper_durable_journal_review::{
           append_terminal_v2_if_prev,append_snapshot_if_prev,ReviewSnapshot,
         },
         native_paper_owner_transaction_review::PaperOwner,
       };
       use std::os::unix::process::ExitStatusExt;
       use std::process::{Command,Stdio};
       let (dir,store,v2_file)=setup(Kind::ExactTargetShort);
       let v=read(&store);let obs=close();
       let mut paper=paper_owner_with_gate(
         &dir,build_v2_compatible_paper(&v,&obs,false));
       let parent=paper.last_seq;
       store.attach_paper_v2_alignment_review(&mut paper,"BNT",parent,&obs).unwrap();
       let expected=paper.last_seq;
       let valid_snapshot=ReviewSnapshot::from_paper(&paper.portfolio,&paper.gate_by_base).unwrap();
       let status=Command::new(std::env::current_exe().unwrap())
         .arg("--exact")
         .arg("v2_four_leg_native_close_tests::p002_terminal_v2_sigkill_child")
         .env("ASTRA_P002_V2_JOURNAL_KILL",&paper.journal)
         .env("ASTRA_P002_V2_OWNER_KILL",&v2_file)
         .stdout(Stdio::null()).stderr(Stdio::null()).status().unwrap();
       assert_eq!(status.signal(),Some(9));
       let cold=PaperOwner::inspect_recovery(&paper.journal).unwrap().unwrap();
       assert_eq!(cold.sequence,expected+1);
       assert_eq!(cold.sealed_terminal_v2.len(),1);
       assert!(cold.state.positions["BNT"].quarantine);
       assert!(!cold.may_resume_paper&&!cold.may_trade_real);
       assert_eq!(cold.state.closed,0);
       assert_eq!(cold.state.modeled_pnl_usdt,0.0);
       assert!(append_terminal_v2_if_prev(
          &paper.journal,"BNT",cold.sequence,&v,&obs).is_err());
       assert!(append_snapshot_if_prev(
          &paper.journal,&valid_snapshot,cold.sequence).is_err());
       fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn r90_r14_same_intent_client_different_original_owner_or_leg_denied(){
       use spotarb_native_market_core::{
         execution::{IntentState,Leg},
         execution_store::{DurableIntentStore,DurableMutationGate},
         native_paper_owner_transaction_review::PaperOwner,
         native_paper_v2_close_source_archive_review::{
           write_local_close_source_once,LocalArchiveFailpoint,
         },
         native_r15_original_owner_binding_review::{
           prepare_r15_original_owner_binding as prepare_owner,
           persist_r15_original_owner_binding_once as publish_owner,
         },
         native_r14_origination_claim_review::{
           derive_r14_origination_claim as derive,
           check_r14_origination_claim as check,
           publish_r14_origination_claim_once as publish,
           read_r14_origination_claim as read_claim,
           inspect_r90_seven_source_cold_claim as cold,
         },
         position_lifecycle::{
           PositionLifecycle,PendingCloseIntent,CloseLeg,DurableLifecycleStore,
         },
       };
       let (dir,v2_store,v2_file)=setup(Kind::ExactTargetShort);
       let v=read(&v2_store);let obs=close();
       let archive=dir.join("r90_close_archive.json");
       write_local_close_source_once(&archive,&v,&obs,
          LocalArchiveFailpoint::None).unwrap();
       let mut paper=paper_owner_with_gate(
         &dir,build_v2_compatible_paper(&v,&obs,false));
       let before=paper.last_seq;
       v2_store.attach_paper_v2_archived_alignment_review(
         &mut paper,"BNT",before,&archive).unwrap();
       let recovered=PaperOwner::inspect_recovery(&paper.journal)
         .unwrap().unwrap();
       let target=v.genesis.planned_net_short_atoms_e12;
       let mut life=PositionLifecycle::new_hedged(target,99).unwrap();
       life.start_close().unwrap();
       life.pending=Some(PendingCloseIntent{
          intent_id:obs.futures_cover_intent_id.clone(),
          client_id:obs.futures_cover_client_id.clone(),
          leg:CloseLeg::Futures,requested_units:target,
          recovery_terminal_filled_units:None,
         recovery_original_exchange_order_id:Some(obs.futures_cover_order_id.clone()),
       });
       let r15_file=dir.join("r90_r15.json");
       DurableLifecycleStore::open(&r15_file).unwrap().persist(&life).unwrap();
       let owner=prepare_owner(&life,&v).unwrap();
       let r87_file=dir.join("r90_original_owner.json");
       publish_owner(&r87_file,&owner).unwrap();
       let r14_file=dir.join("r90_r14.json");
       let mut gate=DurableMutationGate::open(&r14_file).unwrap();
       let intent=obs.futures_cover_intent_id.as_str();
       let client=obs.futures_cover_client_id.as_str();
       gate.begin(intent,Leg::Futures,client).unwrap();
       let row=gate.store.load().unwrap().pop().unwrap();
       let claim=derive(&row,&life,&owner).unwrap();
       let r90_file=dir.join("r90_r14_origination.json");
       publish(&r90_file,&claim).unwrap();
       assert!(publish(&r90_file,&claim).is_err());
       assert_eq!(read_claim(&r90_file).unwrap(),claim);
       let inspect=||cold(&r90_file,&r14_file,&r15_file,&r87_file,
            &v2_file,&archive,&paper.journal,"BNT",recovered.sequence,
            &recovered.hash);
       let first=inspect().unwrap();
       assert!(first.same_original_local_close_owner);
       assert_eq!(first.r14_state,IntentState::Prepared);
       assert!(!first.exchange_authenticated);
       assert!(!first.may_send_retry_or_resume);
       gate.acknowledge(intent,false).unwrap();
       let ack=gate.store.load().unwrap().pop().unwrap();
       assert_eq!(inspect().unwrap().r14_state,IntentState::Acknowledged);
       assert!(check(&claim,&ack,&life,&owner).is_ok());
       // Same R14 IDs can be reinterpreted as an OTHER close leg if the
       // leg was not pinned in a write-once origin certificate.
       let mut swapped_leg=life.clone();
       swapped_leg.pending.as_mut().unwrap().leg=CloseLeg::Spot;
       assert!(check(&claim,&ack,&swapped_leg,&owner).is_err());
       let mut swapped_generation=life.clone();
       swapped_generation.safety_generation+=1;
       assert!(check(&claim,&ack,&swapped_generation,&owner).is_err());
       let mut swapped_amount=life.clone();
       swapped_amount.pending.as_mut().unwrap().requested_units=target-1;
       assert!(check(&claim,&ack,&swapped_amount,&owner).is_err());
       let mut foreign_owner=owner.clone();
       foreign_owner.original_spot_order_id="OTHER-OKX-ENTRY".into();
       assert!(check(&claim,&ack,&life,&foreign_owner).is_err());
       let mut foreign_genesis=owner.clone();
       foreign_genesis.v2_immutable_genesis_sha256="0".repeat(64);
       assert!(check(&claim,&ack,&life,&foreign_genesis).is_err());
       let mut foreign_claim=claim.clone();
       foreign_claim.venue="OKX_SPOT_SELL_CLOSE".into();
       assert!(check(&foreign_claim,&ack,&life,&owner).is_err());
       let mut elevated=claim.clone();
       elevated.may_send_or_retry=true;
       assert!(check(&elevated,&ack,&life,&owner).is_err());
       // A signed-looking JSON checksum must not make a foreign
       // source or obsolete R14 terminal state authoritative.
       let mut rejected=ack.clone();rejected.state=IntentState::Reconciled;
       assert!(check(&claim,&rejected,&life,&owner).is_err());
       let bytes=std::fs::read(&r90_file).unwrap();
       let mut damaged=bytes.clone();let mid=damaged.len()/2;damaged[mid]^=1;
       std::fs::write(&r90_file,&damaged).unwrap();
       assert!(inspect().is_err());
       std::fs::write(&r90_file,&bytes).unwrap();
       assert!(inspect().is_ok());
       let interrupted=r90_file.with_extension("json.incomplete");
       std::fs::write(&interrupted,b"partial").unwrap();
       assert!(inspect().is_err());
       std::fs::remove_file(interrupted).unwrap();
       let symlink=dir.join("r90_alias.json");
       std::os::unix::fs::symlink(&r90_file,&symlink).unwrap();
       assert!(cold(&symlink,&r14_file,&r15_file,&r87_file,
         &v2_file,&archive,&paper.journal,"BNT",recovered.sequence,
         &recovered.hash).is_err());
       assert!(inspect().is_ok());
       fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn r89_r14_ack_before_r15_fsync_crash_requires_exact_pending_owner(){
       use spotarb_native_market_core::{
        execution::{Intent,IntentState,Leg},
        execution_store::{DurableIntentStore,DurableMutationGate},
        native_paper_owner_transaction_review::PaperOwner,
        native_paper_v2_close_source_archive_review::{
          write_local_close_source_once,LocalArchiveFailpoint,
        },
        native_r15_original_owner_binding_review::{
          prepare_r15_original_owner_binding as prepare,
          persist_r15_original_owner_binding_once as publish,
        },
        native_r14_r15_cross_store_cold_review::
           inspect_r89_r14_r15_v2_paper_cold_ack as inspect,
        position_lifecycle::{
          PositionLifecycle,PendingCloseIntent,CloseLeg,DurableLifecycleStore,
        },
       };
       let (dir,store,v2_path)=setup(Kind::ExactTargetShort);
       let v=read(&store);let obs=close();
       let close_archive=dir.join("r89_close_archive.json");
       write_local_close_source_once(&close_archive,&v,&obs,
          LocalArchiveFailpoint::None).unwrap();
       let mut paper=paper_owner_with_gate(
          &dir,build_v2_compatible_paper(&v,&obs,false));
       let parent=paper.last_seq;
       store.attach_paper_v2_archived_alignment_review(
          &mut paper,"BNT",parent,&close_archive).unwrap();
       let cold=PaperOwner::inspect_recovery(&paper.journal).unwrap().unwrap();
       let q=v.genesis.planned_net_short_atoms_e12;
       let mut life=PositionLifecycle::new_hedged(q,99).unwrap();
       life.start_close().unwrap();
       life.pending=Some(PendingCloseIntent{
         intent_id:obs.futures_cover_intent_id.clone(),
         client_id:obs.futures_cover_client_id.clone(),
         leg:CloseLeg::Futures,requested_units:q,
         recovery_terminal_filled_units:None,
         recovery_original_exchange_order_id:Some(obs.futures_cover_order_id.clone()),
       });
       let r15=dir.join("r89_r15.json");
       let r15_store=DurableLifecycleStore::open(&r15).unwrap();
       r15_store.persist(&life).unwrap();
       let owner=dir.join("r89_original_owner.json");
       publish(&owner,&prepare(&life,&v).unwrap()).unwrap();
       let r14=dir.join("r89_r14_intents.json");
       let mut gate=DurableMutationGate::open(&r14).unwrap();
       let intent=obs.futures_cover_intent_id.as_str();
       let client=obs.futures_cover_client_id.as_str();
       gate.begin(intent,Leg::Futures,client).unwrap();
       let probe=||inspect(&r14,&r15,&owner,&v2_path,
          &close_archive,&paper.journal,"BNT",cold.sequence,&cold.hash);
       let prepared=probe().unwrap();
       assert_eq!(prepared.original_pending_state,IntentState::Prepared);
       assert!(prepared.r14_pending_reconciliation_required);
       assert!(!prepared.may_send_or_retry_or_resume);
       gate.acknowledge(intent,false).unwrap();
       drop(gate);
       // Simulated death AFTER original exchange ACK was locally written,
       // but BEFORE R15 recorded any terminal-futures fill observation.
       let reopened=DurableMutationGate::open(&r14).unwrap();
       assert!(reopened.after_durable_prepare(intent).is_err());
       assert!(DurableMutationGate::open(&r14).unwrap()
          .begin("SECOND",Leg::Futures,"SECOND_CLIENT").is_err());
       assert_eq!(probe().unwrap().original_pending_state,
          IntentState::Acknowledged);
       assert!(probe().unwrap().r14_ack_does_not_prove_terminal_fill);
       assert!(!probe().unwrap().verified_closed_or_flat);
       // A claimed UNKNOWN remains an unresolved original action.
       let original=DurableIntentStore::open(&r14).unwrap();
       original.persist(&[Intent{id:intent.into(),
          client_order_id:Some(client.into()),state:IntentState::Unknown}]).unwrap();
       assert_eq!(probe().unwrap().original_pending_state,IntentState::Unknown);
       for terminal in [IntentState::Reconciled,IntentState::Rejected]{
         original.persist(&[Intent{id:intent.into(),
          client_order_id:Some(client.into()),state:terminal}]).unwrap();
         assert!(probe().is_err(),
           "terminal R14 state cannot certify still-pending R15 original");
       }
       original.persist(&[Intent{id:intent.into(),
          client_order_id:Some("OTHER_CLIENT".into()),
          state:IntentState::Acknowledged}]).unwrap();
       assert!(probe().is_err());
       original.persist(&[Intent{id:"OTHER_INTENT".into(),
          client_order_id:Some(client.into()),
          state:IntentState::Acknowledged}]).unwrap();
       assert!(probe().is_err());
       original.persist(&[
         Intent{id:intent.into(),client_order_id:Some(client.into()),
          state:IntentState::Acknowledged},
         Intent{id:"FOREIGN_OPEN".into(),client_order_id:Some("C2".into()),
          state:IntentState::Unknown},
       ]).unwrap();
       assert!(probe().is_err(),"another unresolved R14 action must deny");
       original.persist(&[
         Intent{id:intent.into(),client_order_id:Some(client.into()),
          state:IntentState::Acknowledged},
         Intent{id:intent.into(),client_order_id:Some(client.into()),
          state:IntentState::Acknowledged},
       ]).unwrap();
       assert!(probe().is_err(),"duplicate R14 durable ACK cannot be owner");
       original.persist(&[Intent{id:intent.into(),
          client_order_id:Some(client.into()),
          state:IntentState::Acknowledged}]).unwrap();
       assert!(probe().is_ok());
       let staged=r14.with_extension("tmp");
       std::fs::write(&staged,b"unfinished R14 ACK fsync").unwrap();
       assert!(probe().is_err(),"R14 staged mutation cannot be invisible");
       std::fs::remove_file(staged).unwrap();
       assert!(probe().is_ok());
       let parked=dir.join("r89_parked_r14");
       std::fs::rename(&r14,&parked).unwrap();
       assert!(probe().is_err(),"missing R14 journal must deny recovery");
       std::fs::rename(parked,&r14).unwrap();
       assert!(probe().is_ok());
       fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn r88_between_five_local_publications_crash_matrix_is_quarantine_only(){
       use spotarb_native_market_core::{
         native_paper_owner_transaction_review::PaperOwner,
         native_paper_v2_close_source_archive_review::{
           write_local_close_source_once,LocalArchiveFailpoint,
         },
         native_r15_cross_store_cold_review::
            inspect_r88_cold_original_close_boundary as audit,
         native_r15_original_owner_binding_review::{
           prepare_r15_original_owner_binding as prepare,
           persist_r15_original_owner_binding_once as publish,
         },
         position_lifecycle::{
           PositionLifecycle,PendingCloseIntent,CloseLeg,DurableLifecycleStore,
         },
       };
       let (dir,store,v2_path)=setup(Kind::ExactTargetShort);
       let v=read(&store);
       let obs=close();
       let archive=dir.join("r88_original_close_archive.json");
       let mut paper=paper_owner_with_gate(&dir,
           build_v2_compatible_paper(&v,&obs,false));
       let parent=paper.last_seq;
       let paper_file=paper.journal.clone();
       write_local_close_source_once(&archive,&v,&obs,LocalArchiveFailpoint::None)
           .unwrap();
       store.attach_paper_v2_archived_alignment_review(
           &mut paper,"BNT",parent,&archive).unwrap();
       let cold=PaperOwner::inspect_recovery(&paper_file).unwrap().unwrap();
       let target=v.genesis.planned_net_short_atoms_e12;
       let mut life=PositionLifecycle::new_hedged(target,99).unwrap();
       life.start_close().unwrap();
       life.pending=Some(PendingCloseIntent{
          intent_id:obs.futures_cover_intent_id.clone(),
          client_id:obs.futures_cover_client_id.clone(),
          leg:CloseLeg::Futures,requested_units:target,
          recovery_terminal_filled_units:None,
         recovery_original_exchange_order_id:Some(obs.futures_cover_order_id.clone()),
       });
       let r15_path=dir.join("r88_r15.json");
       let store_r15=DurableLifecycleStore::open(&r15_path).unwrap();
       store_r15.persist(&life).unwrap();
       let local_cert=prepare(&life,&v).unwrap();
       let cert=dir.join("r88_original_owner.json");
       publish(&cert,&local_cert).unwrap();
       let check=||audit(&r15_path,&cert,&v2_path,&archive,&paper_file,
                         "BNT",cold.sequence,&cold.hash);
       let exact=check().unwrap();
       assert!(exact.five_local_files_stable_during_read);
       assert!(exact.original_r15_owner_exact_local_match);
       assert!(exact.source_archive_is_valid_local_witness);
       assert!(exact.original_close_pending_reconciliation_required);
       assert!(!exact.atomic_cross_store_commit_proven);
       assert!(!exact.independent_exchange_account_truth);
       assert!(!exact.external_antirollback_floor_proven);
       assert!(!exact.may_resume_or_send_or_retry);
       assert!(!exact.verified_closed_or_flat);
       // Independently committed stores can crash between ANY pair of
       // publishes. Every missing file must reject the whole cold join.
       for (index,path) in [&r15_path,&cert,&v2_path,&archive,&paper_file]
           .into_iter().enumerate(){
          let parked=dir.join(format!("r88_crash_removed_{index}.held"));
          std::fs::rename(path,&parked).unwrap();
          assert!(check().is_err(),"missing published source {index} accepted");
          std::fs::rename(parked,path).unwrap();
          assert!(check().is_ok(),"restored local exact fixture {index}");
       }
       // A pre-fsync interrupted lifecycle PREPARED update is
       // not interpreted as the older, complete close-state truth.
       let pending_temp=r15_path.with_extension("tmp");
       std::fs::write(&pending_temp,b"partial-R15-PREPARED").unwrap();
       assert!(check().is_err());
       std::fs::remove_file(pending_temp).unwrap();
       let pending_cert=cert.with_extension("json.incomplete");
       std::fs::write(&pending_cert,b"partial-original-owner").unwrap();
       assert!(check().is_err());
       std::fs::remove_file(pending_cert).unwrap();
       let alternate=dir.join("r88_foreign_owner.json");
       let mut foreign=local_cert.clone();
       foreign.original_spot_order_id="DIFFERENT-OKX-ENTRY-ID".into();
       publish(&alternate,&foreign).unwrap();
       assert!(audit(&r15_path,&alternate,&v2_path,&archive,&paper_file,
                    "BNT",cold.sequence,&cold.hash).is_err(),
          "valid SHA with different owner cannot attach on crash restart");
       let stale=life.clone();
       let mut shifted=stale;
       shifted.safety_generation+=1;
       store_r15.persist(&shifted).unwrap();
       assert!(check().is_err(),"other R15 owner generation must be rejected");
       store_r15.persist(&life).unwrap();
       assert!(check().is_ok());
       assert!(audit(&r15_path,&cert,&v2_path,&archive,&paper_file,
                    "BNT",cold.sequence-1,&cold.hash).is_err());
       assert!(audit(&r15_path,&cert,&v2_path,&archive,&paper_file,
                    "BNT",cold.sequence,&"0".repeat(64)).is_err());
       // Even the ideal full local match is QUARANTINED and requires
       // out-of-band exchange and antirollback truth for any real action.
       assert!(!check().unwrap().may_resume_or_send_or_retry);
       std::fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn r87_same_generation_quantity_but_other_original_order_denied(){
       use spotarb_native_market_core::{
         position_lifecycle::{PositionLifecycle,PendingCloseIntent,CloseLeg,DurableLifecycleStore},
         native_paper_owner_transaction_review::PaperOwner,
         native_r15_original_owner_binding_review::{
           prepare_r15_original_owner_binding as prepare,
           persist_r15_original_owner_binding_once as publish,
           read_r15_original_owner_binding as load,
           check_r15_original_owner_binding as check,
           inspect_r87_cold_owner_join as cold_join,
         },
       };
       let (dir,store,_)=setup(Kind::ExactTargetShort);
       let v=read(&store);let obs=close();
       let mut paper=paper_owner_with_gate(
          &dir,build_v2_compatible_paper(&v,&obs,false));
       let parent=paper.last_seq;
       store.attach_paper_v2_alignment_review(&mut paper,"BNT",parent,&obs)
           .unwrap();
       let recovered=PaperOwner::inspect_recovery(&paper.journal)
           .unwrap().unwrap();
       let qty=v.genesis.planned_net_short_atoms_e12;
       let mut life=PositionLifecycle::new_hedged(qty,99).unwrap();
       life.start_close().unwrap();
       life.pending=Some(PendingCloseIntent{
         intent_id:obs.futures_cover_intent_id.clone(),
         client_id:obs.futures_cover_client_id.clone(),
         leg:CloseLeg::Futures,requested_units:qty,
         recovery_terminal_filled_units:None,
         recovery_original_exchange_order_id:Some(obs.futures_cover_order_id.clone()),
       });
       let store_r15=DurableLifecycleStore::open(dir.join("r87_lifecycle.json"))
           .unwrap();
       store_r15.persist(&life).unwrap();
       let cold_life=store_r15.load_for_recovery().unwrap().unwrap();
       let same=prepare(&cold_life,&v).unwrap();
       assert_eq!(same.safety_generation,99);
       assert_eq!(same.target_units_e12,qty);
       assert!(!same.independently_authenticated);
       let cert_path=dir.join("r87_original_owner.json");
       publish(&cert_path,&same).unwrap();
       assert!(publish(&cert_path,&same).is_err(),
          "reusing old owner certificate must not replace it");
       assert_eq!(load(&cert_path).unwrap(),same);
       let valid=cold_join(&cert_path,&cold_life,&recovered,"BNT",&v,&obs)
           .unwrap();
       assert!(valid.local_original_order_identity_matches);
       assert!(!valid.may_retry_or_resume&&!valid.independent_exchange_proof);
       assert!(!valid.externally_antirollback_proven);
       // A DIFFERENT original owner can have the SAME generation and
       // 0.089 BASE size. Its independently self-checksummed local
       // certificate still may not be joined with this V2/PAPER source.
       let mut foreign=same.clone();
       foreign.original_spot_order_id="OTHER-OKX-ORIGINAL-ORDER".into();
       let foreign_path=dir.join("r87_other_owner.json");
       publish(&foreign_path,&foreign).unwrap();
       assert_eq!(load(&foreign_path).unwrap().safety_generation,99);
       assert_eq!(load(&foreign_path).unwrap().target_units_e12,qty);
       assert!(check(&load(&foreign_path).unwrap(),&cold_life,&v).is_err());
       assert!(cold_join(&foreign_path,&cold_life,&recovered,
          "BNT",&v,&obs).is_err());
       let mut other_intent=same.clone();
       other_intent.original_spot_client_id="OTHER-CLIENT".into();
       assert!(check(&other_intent,&cold_life,&v).is_err());
       let mut other_v2=v.clone();
       other_v2.genesis.checkpoint.original.spot_order_id="OTHER-ORDER".into();
       assert!(check(&same,&cold_life,&other_v2).is_err());
       let mut stale=cold_life.clone();stale.safety_generation+=1;
       assert!(check(&same,&stale,&v).is_err());
       let mut wrong_qty=cold_life.clone();wrong_qty.target_units+=1;
       assert!(check(&same,&wrong_qty,&v).is_err());
       let bytes=std::fs::read(&cert_path).unwrap();
       let mut bad=bytes.clone();let middle=bad.len()/2;bad[middle]^=1;
       std::fs::write(&cert_path,&bad).unwrap();
       assert!(load(&cert_path).is_err());
       std::fs::write(&cert_path,&bytes).unwrap();
       assert_eq!(load(&cert_path).unwrap(),same);
       let stage=dir.join("r87_original_owner.json.incomplete");
       std::fs::write(&stage,b"unfinished").unwrap();
       assert!(load(&cert_path).is_err());
       std::fs::remove_file(stage).unwrap();
       let link=dir.join("r87_symlink.json");
       std::os::unix::fs::symlink(&cert_path,&link).unwrap();
       assert!(load(&link).is_err());
       fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn r86_r15_v2_paper_cold_pending_join_rejects_stale_or_foreign_close(){
         use spotarb_native_market_core::{
           native_paper_owner_transaction_review::PaperOwner,
           native_paper_v2_closure_bridge_review::
             inspect_r15_v2_paper_recovery_join as inspect,
           position_lifecycle::{
             PositionLifecycle,DurableLifecycleStore,
             LifecycleState,PendingCloseIntent,CloseLeg,
           },
         };
         let (dir,store,_)=setup(Kind::ExactTargetShort);
         let v=read(&store);let obs=close();
         let mut paper=paper_owner_with_gate(
            &dir,build_v2_compatible_paper(&v,&obs,false));
         let parent=paper.last_seq;
         store.attach_paper_v2_alignment_review(&mut paper,"BNT",parent,&obs)
            .unwrap();
         let cold_paper=PaperOwner::inspect_recovery(&paper.journal)
            .unwrap().unwrap();
         let target=v.genesis.planned_net_short_atoms_e12;
         let mut life=PositionLifecycle::new_hedged(target,99).unwrap();
         life.start_close().unwrap();
         life.pending=Some(PendingCloseIntent{
            intent_id:obs.futures_cover_intent_id.clone(),
            client_id:obs.futures_cover_client_id.clone(),
            leg:CloseLeg::Futures,requested_units:target,
            recovery_terminal_filled_units:None,
            recovery_original_exchange_order_id:None,
         });
         let r15_store=DurableLifecycleStore::open(dir.join("r86_r15.json"))
            .unwrap();
         r15_store.persist(&life).unwrap();
         let mut cold=r15_store.load_for_recovery().unwrap().unwrap();
         assert_eq!(cold.state,LifecycleState::RecoveryRequired);
         assert!(inspect(&cold,&cold_paper,"BNT",&v,&obs).is_err());
         assert!(cold.stage_recovery_original_exchange_order_id_review(
             &r15_store,"INVALID ORDER ID").is_err());
         cold.stage_recovery_original_exchange_order_id_review(
             &r15_store,&obs.futures_cover_order_id).unwrap();
         cold.stage_recovery_original_exchange_order_id_review(
             &r15_store,&obs.futures_cover_order_id).unwrap();
         assert!(cold.stage_recovery_original_exchange_order_id_review(
             &r15_store,"FOREIGN_ORDER_ID").is_err());
         let cold=r15_store.load_for_recovery().unwrap().unwrap();
         assert_eq!(cold.pending.as_ref().unwrap()
             .recovery_original_exchange_order_id.as_deref(),
             Some(obs.futures_cover_order_id.as_str()));
         let result=inspect(&cold,&cold_paper,"BNT",&v,&obs).unwrap();
         assert_eq!(result.locally_matched_generation,99);
         assert!(result.identical_local_pending_intent);
         assert!(result.r15_pending_reconciliation_required);
         assert!(!result.cross_store_atomic_durability_proven);
         assert!(!result.immutable_r15_original_owner_order_id_proven);
         assert!(!result.independent_exchange_account_truth);
         assert!(!result.may_retry_or_resume&&!result.verified_closed);
         let mut absent_exchange_id=cold.clone();
         absent_exchange_id.pending.as_mut().unwrap().recovery_original_exchange_order_id=None;
         assert!(inspect(&absent_exchange_id,&cold_paper,"BNT",&v,&obs).is_err());
         let mut foreign_exchange_id=cold.clone();
         foreign_exchange_id.pending.as_mut().unwrap().recovery_original_exchange_order_id=
             Some("OTHER_EXCHANGE_ORDER".into());
         assert!(inspect(&foreign_exchange_id,&cold_paper,"BNT",&v,&obs).is_err());
         let mut stale=cold.clone();stale.safety_generation+=1;
         assert!(inspect(&stale,&cold_paper,"BNT",&v,&obs).is_err());
         let mut foreign=cold.clone();
         foreign.pending.as_mut().unwrap().client_id="OTHER_OWNER".into();
         assert!(inspect(&foreign,&cold_paper,"BNT",&v,&obs).is_err());
         let mut bad_amount=cold.clone();bad_amount.target_units+=1;
         assert!(inspect(&bad_amount,&cold_paper,"BNT",&v,&obs).is_err());
         let mut changed_leg=cold.clone();
         changed_leg.pending.as_mut().unwrap().leg=CloseLeg::Spot;
         assert!(inspect(&changed_leg,&cold_paper,"BNT",&v,&obs).is_err());
         let mut wrong_qty=cold.clone();
         wrong_qty.pending.as_mut().unwrap().requested_units=target-1;
         assert!(inspect(&wrong_qty,&cold_paper,"BNT",&v,&obs).is_err());
         // A staged R15 terminal quantity must equal the immutable
         // source-matched PAPER closing fill, not merely fit its request.
         let mut staged_full=cold.clone();
         staged_full.pending.as_mut().unwrap().recovery_terminal_filled_units=Some(target);
         assert!(inspect(&staged_full,&cold_paper,"BNT",&v,&obs).is_ok());
         let mut staged_partial=cold.clone();
         staged_partial.pending.as_mut().unwrap().recovery_terminal_filled_units=Some(target-1);
         assert!(inspect(&staged_partial,&cold_paper,"BNT",&v,&obs).is_err());
         let mut misordered=cold.clone();misordered.spot_filled_units=1;
         assert!(inspect(&misordered,&cold_paper,"BNT",&v,&obs).is_err());
         let mut claimed_closed=cold.clone();
         claimed_closed.state=LifecycleState::Closed;
         claimed_closed.pending=None;
         assert!(inspect(&claimed_closed,&cold_paper,"BNT",&v,&obs).is_err());
         let mut missing=cold.clone();missing.pending=None;
         assert!(inspect(&missing,&cold_paper,"BNT",&v,&obs).is_err());
         let mut wrong_source=v.clone();
         wrong_source.genesis.original_owner_generation+=1;
         assert!(inspect(&cold,&cold_paper,"BNT",&wrong_source,&obs).is_err());
         // The R15 second-leg intent can be locally compared only if
         // futures close has already been fully observed by R15.
         let mut spot=cold.clone();
         spot.pending.as_mut().unwrap().leg=CloseLeg::Spot;
         spot.pending.as_mut().unwrap().intent_id=obs.spot_sell_intent_id.clone();
         spot.pending.as_mut().unwrap().client_id=obs.spot_sell_client_id.clone();
         spot.pending.as_mut().unwrap().recovery_original_exchange_order_id=
             Some(obs.spot_sell_order_id.clone());
         spot.futures_filled_units=target;
         let spot_result=inspect(&spot,&cold_paper,"BNT",&v,&obs).unwrap();
         assert_eq!(spot_result.pending_leg,CloseLeg::Spot);
         assert!(!spot_result.may_retry_or_resume);
         let mut spot_staged_full=spot.clone();
         spot_staged_full.pending.as_mut().unwrap().recovery_terminal_filled_units=Some(target);
         assert!(inspect(&spot_staged_full,&cold_paper,"BNT",&v,&obs).is_ok());
         let mut spot_staged_partial=spot.clone();
         spot_staged_partial.pending.as_mut().unwrap().recovery_terminal_filled_units=Some(target-1);
         assert!(inspect(&spot_staged_partial,&cold_paper,"BNT",&v,&obs).is_err());
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn v2_full_source_tape_binds_to_same_paper_cas_without_authority(){
         use spotarb_native_market_core::{
           native_paper_owner_transaction_review::PaperOwner,
           native_paper_v2_closure_bridge_review::inspect_paper_v2_alignment,
         };
         let (d,s,_p)=setup(Kind::ExactTargetShort);
         let v=read(&s);let obs=close();
         let gate=build_v2_compatible_paper(&v,&obs,false);
         let mut owner=paper_owner_with_gate(&d,gate);
         let parent=owner.last_seq;
         let p=inspect_paper_v2_alignment(
             &owner.gate_by_base["BNT"],&v,&obs,parent).unwrap();
         assert_eq!(p.original_generation,99);
         assert!(!p.verified_closed&&!p.may_send_or_retry&&!p.independent_exchange_truth);
         let forged=p.clone();
         assert!(owner.transact(move|_,g|{
             g.get_mut("BNT").unwrap().v2_local_alignment=Some(forged);Ok(())
         }).is_err());
         assert!(owner.gate_by_base["BNT"].v2_local_alignment.is_none());
         assert!(s.attach_paper_v2_alignment_review(&mut owner,"BNT",parent+1,&obs).is_err());
         assert_eq!(owner.last_seq,parent);
         assert_eq!(s.attach_paper_v2_alignment_review(&mut owner,"BNT",parent,&obs).unwrap(),parent+1);
         assert!(s.attach_paper_v2_alignment_review(&mut owner,"BNT",parent+1,&obs).is_err());
         let recovered=PaperOwner::inspect_recovery(&owner.journal).unwrap().unwrap();
         let r=&recovered.state.positions["BNT"].fills;
         assert_eq!(r.v2_local_alignment.as_ref(),Some(&p));
         assert!(r.restart_quarantine);
         assert!(!recovered.may_resume_paper&&!recovered.may_trade_real);
         assert_eq!(recovered.state.closed,0);
         use spotarb_native_market_core::native_paper_v2_closure_bridge_review::
             inspect_recovered_alignment_consistency;
         let check=inspect_recovered_alignment_consistency(
             &recovered,"BNT",&v,&obs).unwrap();
         assert!(check.source_agrees_with_local_journal);
         assert!(!check.may_resume_paper&&!check.may_trade_real&&!check.verified_closed);
         let mut wrong_fee=v.clone();
         wrong_fee.genesis.checkpoint.source_shaped_fee_fills[0].signed_fee_e12-=1;
         assert!(inspect_recovered_alignment_consistency(
             &recovered,"BNT",&wrong_fee,&obs).is_err());
         let mut wrong_generation=v.clone();
         wrong_generation.genesis.original_owner_generation+=1;
         assert!(inspect_recovered_alignment_consistency(
             &recovered,"BNT",&wrong_generation,&obs).is_err());
         let mut forged_digest=PaperOwner::inspect_recovery(&owner.journal)
             .unwrap().unwrap();
         forged_digest.state.positions.get_mut("BNT").unwrap()
             .fills.v2_local_alignment.as_mut().unwrap()
             .source_digest_sha256="0".repeat(64);
         assert!(inspect_recovered_alignment_consistency(
             &forged_digest,"BNT",&v,&obs).is_err());
         assert!(owner.transact(|_,g|{
             g.get_mut("BNT").unwrap().v2_local_alignment=None;Ok(())
         }).is_err());
         fs::remove_dir_all(d).unwrap();
     }
     // True separate PIDs, not threads. Failpoints are at the method boundary:
     // pre-CAS vs after append+file+directory fsync, BEFORE caller ACK.
     #[test]
     fn bridge_child_worker(){
         let Ok(mode)=std::env::var("SPOTARB_AST_BRIDGE_CHILD") else{return};
         let root=std::path::PathBuf::from(
             std::env::var("SPOTARB_AST_BRIDGE_DEST").unwrap());
         let (temp,store,_)=setup(Kind::ExactTargetShort);
         let v=read(&store);let obs=close();
         let gate=build_v2_compatible_paper(&v,&obs,false);
         let mut owner=paper_owner_with_gate(&root,gate);
         let seq=owner.last_seq;
         if mode=="before" {
             spotarb_native_market_core::native_paper_v2_closure_bridge_review::
               inspect_paper_v2_alignment(&owner.gate_by_base["BNT"],&v,&obs,seq)
               .unwrap();
             std::process::exit(71);
         }
         if mode=="after"{
             assert_eq!(store.attach_paper_v2_alignment_review(&mut owner,"BNT",seq,&obs).unwrap(),
                 seq+1);
             std::process::exit(72);
         }
         panic!("unsupported child mode");
     }
     fn run_bridge_child(mode:&str,root:&std::path::Path)->i32{
         use std::process::{Command,Stdio};
         Command::new(std::env::current_exe().unwrap())
           .arg("--exact")
           .arg("v2_four_leg_native_close_tests::bridge_child_worker")
           .arg("--nocapture")
           .env("SPOTARB_AST_BRIDGE_CHILD",mode)
           .env("SPOTARB_AST_BRIDGE_DEST",root)
           .stdout(Stdio::null()).stderr(Stdio::null())
           .status().unwrap().code().unwrap()
     }
     #[test]
     fn crash_before_alignment_cas_and_after_fsync_are_quarantine_only(){
         use spotarb_native_market_core::native_paper_owner_transaction_review::PaperOwner;
         for (mode,exit_code,expected_seq,aligned) in [
             ("before",71,2,false),("after",72,3,true)]{
             let dir=std::env::temp_dir().join(format!(
                 "astra_bridge_boundary_{}_{}_{}",std::process::id(),
                 mode,NEXT.fetch_add(1,Ordering::Relaxed)));
             fs::create_dir(&dir).unwrap();
             assert_eq!(run_bridge_child(mode,&dir),exit_code);
             let path=dir.join("paper.jsonl");
             let cold=PaperOwner::inspect_recovery(&path).unwrap().unwrap();
             assert_eq!(cold.sequence,expected_seq);
             assert_eq!(cold.state.positions["BNT"].fills.v2_local_alignment.is_some(),
                 aligned);
             assert_eq!(cold.state.closed,0);
             assert!(!cold.may_resume_paper&&!cold.may_trade_real);
             assert!(cold.state.positions["BNT"].fills.restart_quarantine);
             assert!(PaperOwner::start_new(&path,1_780_000_005_500).is_err());
             if aligned{
                 let (temp,store,_)=setup(Kind::ExactTargetShort);
                 let preview=spotarb_native_market_core::
                   native_paper_v2_closure_bridge_review::
                   inspect_recovered_alignment_consistency(&cold,"BNT",&read(&store),
                       &close()).unwrap();
                 assert!(preview.source_agrees_with_local_journal);
                 assert!(!preview.may_resume_paper&&!preview.verified_closed);
                 fs::remove_dir_all(temp).unwrap();
             }
             fs::remove_dir_all(dir).unwrap();
         }
     }
     #[test]
     fn blocked_alignment_journal_fails_closed_without_publishing_marker(){
         let (d,s,_)=setup(Kind::ExactTargetShort);
         let v=read(&s);let obs=close();
         let gate=build_v2_compatible_paper(&v,&obs,false);
         let mut owner=paper_owner_with_gate(&d,gate);
         let prior=owner.last_seq;
         let lock=owner.journal.with_extension("writer_lock");
         fs::write(&lock,b"synthetic-second-writer").unwrap();
         assert!(s.attach_paper_v2_alignment_review(&mut owner,"BNT",prior,&obs).is_err());
         assert!(owner.frozen);
         assert_eq!(owner.last_seq,prior);
         let cold=spotarb_native_market_core::native_paper_owner_transaction_review::
             PaperOwner::inspect_recovery(&owner.journal).unwrap().unwrap();
         assert_eq!(cold.sequence,prior);
         assert!(cold.state.positions["BNT"].fills.v2_local_alignment.is_none());
         fs::remove_file(lock).unwrap();
         assert!(s.attach_paper_v2_alignment_review(&mut owner,"BNT",prior,&obs).is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn coherent_local_v2_and_paper_rollback_needs_external_paper_floor(){
         use spotarb_native_market_core::{
             native_paper_durable_journal_review::{
                 inspect_journal_against_supplied_floor,read_journal,
             },
             native_trade2f3_v2_versioned_store::V2ReviewPhase,
         };
         let (dir,store,v2_path)=setup(Kind::ExactTargetShort);
         let owner_v2=read(&store);
         let observation=close();
         let gate=build_v2_compatible_paper(&owner_v2,&observation,false);
         let mut paper=paper_owner_with_gate(&dir,gate);
         let paper_path=paper.journal.clone();
         let old_v2=fs::read(&v2_path).unwrap();
         let old_paper=fs::read(&paper_path).unwrap();
         let old_tip=read_journal(&paper_path).unwrap().unwrap();
         assert_eq!(old_tip.sequence,2);
         let paper_seq=paper.last_seq;
         assert_eq!(store.attach_paper_v2_alignment_review(
             &mut paper,"BNT",paper_seq,&observation).unwrap(),3);
         let trusted_tip=read_journal(&paper_path).unwrap().unwrap();
         assert_eq!(trusted_tip.sequence,3);
         assert!(trusted_tip.state.positions["BNT"].fills.v2_local_alignment.is_some());
         store.advance_v2_review(4,V2ReviewPhase::RecoveryOnly,FP::None).unwrap();
         assert_eq!(read(&store).revision,5);
         // ONLY isolated synthetic fixture files: replace both with older
         // valid snapshots. An attacker with same-disk access could also
         // replace the local floor, which is precisely the unmet requirement.
         fs::write(&v2_path,old_v2).unwrap();
         fs::write(&paper_path,old_paper).unwrap();
         assert_eq!(read(&store).revision,4);
         let rolled=read_journal(&paper_path).unwrap().unwrap();
         assert_eq!(rolled.sequence,2);
         assert!(!rolled.may_resume_paper&&!rolled.may_trade_real);
         // A PRE-ROLLBACK independently supplied exact tip rejects BOTH-file
         // coherent rollback, even though each old local object is valid.
         assert!(inspect_journal_against_supplied_floor(
             &paper_path,trusted_tip.sequence,&trusted_tip.hash).is_err());
         // Explicit security boundary: if a caller also rolls back the floor,
         // this check CANNOT discover a coherent rollback on the same host.
         let unsafe_local_old_floor=inspect_journal_against_supplied_floor(
             &paper_path,old_tip.sequence,&old_tip.hash).unwrap();
         assert!(!unsafe_local_old_floor.may_resume_paper);
         assert!(!unsafe_local_old_floor.may_trade_real);
         assert!(inspect_journal_against_supplied_floor(
             &paper_path,0,&trusted_tip.hash).is_err());
         assert!(inspect_journal_against_supplied_floor(
             &paper_path,trusted_tip.sequence,"badhex").is_err());
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn v2_revision_change_before_paper_commit_denies_binding(){
         let (dir,store,_)=setup(Kind::ExactTargetShort);
         let original=read(&store);let obs=close();
         let gate=build_v2_compatible_paper(&original,&obs,false);
         let mut paper=paper_owner_with_gate(&dir,gate);
         let seq=paper.last_seq;
         store.advance_v2_review(4,Phase::RecoveryOnly,FP::None).unwrap();
         assert!(store.attach_paper_v2_alignment_review(
             &mut paper,"BNT",seq,&obs).is_err());
         assert_eq!(paper.last_seq,seq);
         assert!(paper.gate_by_base["BNT"].v2_local_alignment.is_none());
         let cold=spotarb_native_market_core::
           native_paper_owner_transaction_review::PaperOwner::
           inspect_recovery(&paper.journal).unwrap().unwrap();
         assert!(cold.state.positions["BNT"].fills.v2_local_alignment.is_none());
         assert!(!cold.may_trade_real&&!cold.may_resume_paper);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn v2_flock_serializes_paper_binding_against_review_mutator(){
         use std::{
             fs::OpenOptions,os::fd::AsRawFd,
             sync::{mpsc,Arc,Mutex},
             thread,time::Duration,
         };
         use spotarb_native_market_core::native_trade2f3_v2_versioned_store::V2ReviewPhase;
         use spotarb_native_market_core::native_paper_owner_transaction_review::PaperOwner;
         let (dir,store,path)=setup(Kind::ExactTargetShort);
         let v=read(&store);let obs=close();
         let gate=build_v2_compatible_paper(&v,&obs,false);
         let p=Arc::new(Mutex::new(paper_owner_with_gate(&dir,gate)));
         let paper_seq=p.lock().unwrap().last_seq;
         let lock_path=path.with_extension("lock");
         let held=OpenOptions::new().create(true).read(true).write(true)
             .open(&lock_path).unwrap();
         assert_eq!(unsafe{libc::flock(held.as_raw_fd(),libc::LOCK_EX)},0);
         let (ready_tx,ready_rx)=mpsc::channel();
         let (done_tx,done_rx)=mpsc::channel();
         let store_binder=store.clone();
         let paper_binder=p.clone();
         let binder=thread::spawn(move||{
             ready_tx.send(()).unwrap();
             let mut guard=paper_binder.lock().unwrap();
             let result=store_binder.attach_paper_v2_alignment_review(
               &mut guard,"BNT",paper_seq,&obs);
             done_tx.send(result.is_ok()).unwrap();
         });
         ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
         assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
             "binder cannot bypass existing V2 owner flock");
         assert_eq!(unsafe{libc::flock(held.as_raw_fd(),libc::LOCK_UN)},0);
         assert!(done_rx.recv_timeout(Duration::from_secs(5)).unwrap());
         binder.join().unwrap();
         let journal=p.lock().unwrap().journal.clone();
         let bound=PaperOwner::inspect_recovery(&journal).unwrap().unwrap();
         assert_eq!(bound.sequence,paper_seq+1);
         let checked=spotarb_native_market_core::
             native_paper_v2_closure_bridge_review::
             inspect_recovered_alignment_consistency(
                 &bound,"BNT",&v,&close()).unwrap();
         assert!(checked.source_agrees_with_local_journal);
         // Source is allowed to advance AFTER releasing the V2 lock.
         // This invalidates future local source checks without permitting
         // a retry or changing the historically durable PAPER CAS marker.
         store.advance_v2_review(4,V2ReviewPhase::RecoveryOnly,FP::None)
             .unwrap();
         let latest=read(&store);
         assert!(spotarb_native_market_core::
             native_paper_v2_closure_bridge_review::
             inspect_recovered_alignment_consistency(
                 &bound,"BNT",&latest,&close()).is_err());
         assert!(!bound.may_resume_paper&&!bound.may_trade_real);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn mismatch_on_exact_mock_cover_price_refuses_v2_paper_binding(){
         let (d,s,_p)=setup(Kind::ExactTargetShort);
         let v=read(&s);let obs=close();
         let wrong=build_v2_compatible_paper(&v,&obs,true);
         let mut owner=paper_owner_with_gate(&d,wrong);
         let before=owner.last_seq;
         assert!(s.attach_paper_v2_alignment_review(&mut owner,"BNT",before,&obs).is_err());
         assert_eq!(owner.last_seq,before);
         assert!(owner.gate_by_base["BNT"].v2_local_alignment.is_none());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn base_fee_open_009_minus_0001_equals_0089_close_owner_atomic_and_pnl_provisional(){
         let (d,s,p)=setup(Kind::ExactTargetShort);
         let o=close();
         let local=inspect(&read(&s),&o).unwrap();
         assert_eq!(local.original_spot_buy_gross_e12,a("0.09").unwrap());
         assert_eq!(local.original_base_fee_e12,a("0.001").unwrap());
         assert_eq!(local.owned_net_base_e12,a("0.089").unwrap());
         assert_eq!(local.binance_short_cover_e12,a("0.089").unwrap());
         assert_eq!(local.okx_spot_sell_e12,a("0.089").unwrap());
         assert_eq!(local.local_spot_base_remaining_e12,0);
         assert_eq!(local.local_future_short_remaining_e12,0);
         assert_eq!(local.trade_only_pnl_before_funding_e12,a("0.065").unwrap() as i128);
         assert_eq!(local.definitive_lifetime_net_pnl_e12,None);
         assert!(!local.independently_verified_flat);
         assert!(!local.may_send_retry_or_verified_closed);
         let saved=s.record_v2_four_leg_close_review(4,o.clone(),FP::None).unwrap();
         assert_eq!(saved.revision,5);
         assert_eq!(saved.phase,Phase::FourLegCloseReviewed);
         assert_eq!(saved.close_four_leg,Some(o));
         assert!(!saved.may_send_or_cancel_or_verify_closed);
         assert!(!saved.production_authorized);
         assert_eq!(read(&NativeStateStore::open(&p).unwrap()),saved);
         assert!(s.load().is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn two_real_source_shaped_closing_fills_each_no_fictitious_extra_bill(){
         let (d,s,_p)=setup(Kind::ExactTargetShort);
         let mut o=close();
         let mut second=o.spot_sell_rows[0].clone();
         second.trade_id="9003".into();second.bill_id="303".into();
         second.base_qty_atoms_e12=a("0.039").unwrap();
         second.base_balance_delta_e12=-(a("0.039").unwrap() as i128);
         second.base_balance_after_e12=0;
         second.signed_fee_quote_atoms_e12=-(a("0.004").unwrap() as i128);
         second.occurred_at_ms=1_780_000_004_100;
         o.spot_sell_rows[0].base_qty_atoms_e12=a("0.05").unwrap();
         o.spot_sell_rows[0].base_balance_delta_e12=-(a("0.05").unwrap() as i128);
         o.spot_sell_rows[0].base_balance_after_e12=a("0.039").unwrap();
         o.spot_sell_rows[0].signed_fee_quote_atoms_e12=-(a("0.005").unwrap() as i128);
         o.spot_sell_rows.push(second);
         let p=inspect(&read(&s),&o).unwrap();
         assert_eq!(p.okx_spot_sell_e12,a("0.089").unwrap());
         assert_eq!(p.trade_only_pnl_before_funding_e12,a("0.065").unwrap() as i128);
         assert_eq!(s.record_v2_four_leg_close_review(4,o,FP::None).unwrap().revision,5);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn unknown_or_partial_futures_open_refuses_normal_four_leg_close(){
         for kind in [Kind::UnknownAcknowledgement,Kind::PartialShort]{
             let (d,s,p)=setup(kind);let before=fs::read(&p).unwrap();
             assert!(s.record_v2_four_leg_close_review(4,close(),FP::None).is_err());
             assert_eq!(fs::read(&p).unwrap(),before);
             assert_eq!(read(&s).revision,4);
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn foreign_trade_bill_fee_asset_overhedge_and_base_after_delta_rejected(){
         for mode in 0..13{
             let (d,s,p)=setup(Kind::ExactTargetShort);let mut o=close();
             match mode{
                 0=>o.spot_sell_rows[0].trade_id="9001".into(),
                 1=>o.spot_sell_rows[0].bill_id="301".into(),
                 2=>o.spot_sell_rows[0].bill_id="302x".into(),
                 3=>o.spot_sell_rows[0].order_id="999".into(),
                 4=>o.spot_sell_rows[0].fee_currency="BNT".into(),
                 5=>o.spot_sell_rows[0].base_qty_atoms_e12=a("0.09").unwrap(),
                 6=>o.spot_sell_rows[0].base_balance_delta_e12=0,
                 7=>o.spot_sell_rows[0].base_balance_after_e12=1,
                 8=>o.futures_cover_rows[0].trade_id="88001".into(),
                 9=>o.futures_cover_rows[0].order_id="777".into(),
                 10=>o.futures_cover_rows[0].fee_currency="BNB".into(),
                 11=>o.futures_cover_rows[0].base_qty_atoms_e12=a("0.09").unwrap(),
                 _=>o.futures_cover_rows[0].realized_pnl_quote_atoms_e12=0,
             }
             let before=fs::read(&p).unwrap();
             assert!(s.record_v2_four_leg_close_review(4,o,FP::None).is_err(),"mode {mode}");
             assert_eq!(fs::read(&p).unwrap(),before);
             assert_eq!(read(&s).revision,4);
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn closing_timestamps_balance_and_untrusted_page_completeness_denied(){
         for mode in 0..10{
             let (d,s,_p)=setup(Kind::ExactTargetShort);let mut o=close();
             match mode{
                 0=>o.futures_cover_rows[0].occurred_at_ms=1_780_000_001_000,
                 1=>o.spot_sell_rows[0].occurred_at_ms=1_780_000_002_999,
                 2=>o.spot_bill_window_complete=false,
                 3=>o.spot_terminal_page_seen=false,
                 4=>o.futures_trade_window_complete=false,
                 5=>o.futures_terminal_page_seen=false,
                 6=>o.both_closing_orders_terminal=false,
                 7=>o.first_futures_flat.short_qty_atoms_e12=1,
                 8=>o.second_futures_flat.captured_at_ms=o.first_futures_flat.captured_at_ms,
                 _=>o.first_futures_flat.captured_at_ms=1_780_000_002_500,
             }
             assert!(s.record_v2_four_leg_close_review(4,o,FP::None).is_err(),"mode {mode}");
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn no_fake_global_completeness_funding_or_close_authority_claims(){
         for mode in 0..3{
             let (d,s,_p)=setup(Kind::ExactTargetShort);let mut o=close();
             match mode{
                 0=>o.independent_account_history_authenticated=true,
                 1=>o.late_funding_and_transfers_complete=true,
                 _=>o.trading_or_verified_closed_authorized=true,
             }
             assert!(s.record_v2_four_leg_close_review(4,o,FP::None).is_err());
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn ordinary_close_cannot_claim_verifiedclosed_and_cannot_mutate_old_v1(){
         let (d,s,_p)=setup(Kind::ExactTargetShort);
         let after=s.record_v2_four_leg_close_review(4,close(),FP::None).unwrap();
         assert_eq!(after.revision,5);
         assert!(!after.production_authorized);
         assert!(!after.may_send_or_cancel_or_verify_closed);
         assert_eq!(require_native_mutation_egress(),Err(NativeHardSafetyError::MutationCapabilityNotCertified));
         assert!(s.record_v2_four_leg_close_review(5,close(),FP::None).is_err());
         assert!(s.advance_v2_review(5,Phase::FourLegCloseReviewed,FP::None).is_err());
         let recovery=s.advance_v2_review(5,Phase::RecoveryOnly,FP::None).unwrap();
         assert_eq!(recovery.revision,6);
         assert_eq!(read(&s).close_four_leg,Some(close()));
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn interrupted_close_stage_quarantines_owner_and_never_resends(){
         for fp in [FP::PartialWrite,FP::AfterFileSync]{
             let (d,s,path)=setup(Kind::ExactTargetShort);
             let prior=fs::read(&path).unwrap();
             assert!(s.record_v2_four_leg_close_review(4,close(),fp).is_err());
             assert_eq!(fs::read(&path).unwrap(),prior);
             assert!(pending_path(&path).exists());
             assert!(s.load_versioned_review().is_err());
             assert!(s.record_v2_four_leg_close_review(4,close(),FP::None).is_err());
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn post_rename_directory_fsync_ambiguity_never_acks_or_authorizes(){
         let (d,s,path)=setup(Kind::ExactTargetShort);
         assert!(s.record_v2_four_leg_close_review(4,close(),FP::AfterRename).is_err());
         let seen=read(&NativeStateStore::open(path).unwrap());
         assert_eq!(seen.revision,5);
         assert!(!seen.production_authorized);
         assert!(!seen.may_send_or_cancel_or_verify_closed);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn concurrent_close_cas_five_contenders_one_winner_and_reopen(){
         let (d,s,path)=setup(Kind::ExactTargetShort);
         let barrier=Arc::new(Barrier::new(6));let mut h=Vec::new();
         for _ in 0..5{
             let p=path.clone();let b=barrier.clone();
             h.push(thread::spawn(move||{
                 let s=NativeStateStore::open(p).unwrap();
                 b.wait();s.record_v2_four_leg_close_review(4,close(),FP::None).is_ok()
             }));
         }
         barrier.wait();
         assert_eq!(h.into_iter().map(|h|h.join().unwrap()).filter(|x|*x).count(),1);
         assert_eq!(read(&s).revision,5);
         assert!(!pending_path(&path).exists());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn stale_closed_snapshot_restore_is_still_unprotected_without_external_anchor(){
         let (d,s,path)=setup(Kind::ExactTargetShort);let prior=fs::read(&path).unwrap();
         s.record_v2_four_leg_close_review(4,close(),FP::None).unwrap();
         assert_eq!(read(&s).revision,5);
         fs::write(&path,prior).unwrap();
         assert_eq!(read(&s).revision,4);
         assert!(!read(&s).production_authorized);
         fs::remove_dir_all(d).unwrap();
     }
 }

 // Off-line emergency C5/C6 witness: NEVER promotes VerifiedClosed.
 mod v2_emergency_close_owner_tests {
     use super::*;
     use super::v2_four_leg_native_close_tests::{setup,read,close};
     use spotarb_native_market_core::{
         authoritative_state::NativeStateStore,
         native_trade2f3_v2_versioned_store::{V2ReviewPhase as Phase,V2ReviewFailpoint as FP,pending_path},
         native_trade2f3_v2_c3_futures_evidence::V2FuturesObservationKind as C3Kind,
         native_trade2f3_v2_emergency_close::{
             V2EmergencyLegOutcome as Outcome,V2EmergencyCloseEvidence as Emergency,
             V2EmergencyFuturesCover as Future,V2EmergencySpotSell as Spot,
             inspect_v2_emergency_close as inspect,
         },
     };
     use std::{fs,sync::{Arc,Barrier},thread};
     fn evidence(f:Outcome,s:Outcome)->Emergency{
         let all=close();
         let mut fut=Future{
             outcome:f,intent_id:all.futures_cover_intent_id,
             client_id:all.futures_cover_client_id,
             order_id:Some(all.futures_cover_order_id),
             fills:all.futures_cover_rows,
             position_first:Some(all.first_futures_flat),
             position_second:Some(all.second_futures_flat),
             pages_complete:true,terminal_empty_page_seen:true,
             order_terminal_claimed:false,
         };
         if f==Outcome::UnknownAcknowledgement{
             fut.order_id=None;fut.fills.clear();
             fut.position_first=None;fut.position_second=None;
             fut.pages_complete=false;fut.terminal_empty_page_seen=false;
         }else{
             fut.fills[0].base_qty_atoms_e12=a("0.05").unwrap();
             fut.fills[0].realized_pnl_quote_atoms_e12=a("0.05").unwrap() as i128;
             fut.position_first.as_mut().unwrap().short_qty_atoms_e12=a("0.039").unwrap();
             fut.position_second.as_mut().unwrap().short_qty_atoms_e12=a("0.039").unwrap();
         }
         let mut spot=Spot{
             outcome:s,intent_id:all.spot_sell_intent_id,
             client_id:all.spot_sell_client_id,
             order_id:Some(all.spot_sell_order_id),
             fills:all.spot_sell_rows,pages_complete:true,
             terminal_empty_page_seen:true,order_terminal_claimed:false,
         };
         if s==Outcome::UnknownAcknowledgement{
             spot.order_id=None;spot.fills.clear();
             spot.pages_complete=false;spot.terminal_empty_page_seen=false;
         }else{
             spot.fills[0].base_qty_atoms_e12=a("0.03").unwrap();
             spot.fills[0].base_balance_delta_e12=-(a("0.03").unwrap() as i128);
             spot.fills[0].base_balance_after_e12=a("0.059").unwrap();
         }
         Emergency{
             source_original_generation:99,source_v2_revision:4,
             window_from_ms:1_780_000_002_000,
             captured_at_ms:1_780_000_005_000,
             futures_cover:fut,spot_sell:spot,
             exchange_account_global_history_authenticated:false,
             independent_rollback_floor_verified:false,
             may_retry_send_cancel_or_verify_closed:false,
         }
     }
     fn one(f:Outcome,s:Outcome){
         let (d,store,path)=setup(C3Kind::ExactTargetShort);
         let e=evidence(f,s);
         let p=inspect(&read(&store),&e).unwrap();
         assert!(!p.independently_account_proven);
         assert!(p.definitive_pnl_atoms_e12.is_none());
         assert!(!p.may_send_retry_cancel_or_verify_closed);
         let saved=store.record_v2_emergency_close_review(4,e.clone(),FP::None).unwrap();
         assert_eq!(saved.revision,5);
         assert_eq!(saved.phase,Phase::EmergencyCloseRecorded);
         assert_eq!(saved.emergency_close,Some(e));
         assert!(saved.close_four_leg.is_none());
         assert!(!saved.may_send_or_cancel_or_verify_closed);
         assert!(!saved.production_authorized);
         let cold=NativeStateStore::open(&path).unwrap();
         assert_eq!(read(&cold),saved);
         assert!(store.record_v2_four_leg_close_review(5,close(),FP::None).is_err());
         assert!(store.record_v2_emergency_close_review(5,evidence(f,s),FP::None).is_err());
         let terminal=store.advance_v2_review(5,Phase::RecoveryOnly,FP::None).unwrap();
         assert_eq!(terminal.revision,6);
         assert!(terminal.emergency_close.is_some());
         assert_eq!(read(&store),terminal);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn futures_partial_spot_unknown_preserves_unknown_not_zero(){
         let f=Outcome::PartialExecution;let u=Outcome::UnknownAcknowledgement;
         let (d,s,_)=setup(C3Kind::ExactTargetShort);
         let p=inspect(&read(&s),&evidence(f,u)).unwrap();
         assert_eq!(p.observed_futures_cover_atoms_e12,Some(a("0.05").unwrap()));
         assert_eq!(p.remaining_short_atoms_e12,Some(a("0.039").unwrap()));
         assert_eq!(p.observed_spot_sell_atoms_e12,None);
         assert_eq!(p.remaining_owned_spot_atoms_e12,None);
         assert_eq!(p.known_net_base_minus_short_atoms_e12,None);
         fs::remove_dir_all(d).unwrap();
         one(f,u);
     }
     #[test]
     fn future_unknown_spot_partial_preserves_unknown_exposure(){
         let f=Outcome::UnknownAcknowledgement;let s=Outcome::PartialExecution;
         let (d,owner,_)=setup(C3Kind::ExactTargetShort);
         let p=inspect(&read(&owner),&evidence(f,s)).unwrap();
         assert_eq!(p.observed_futures_cover_atoms_e12,None);
         assert_eq!(p.remaining_short_atoms_e12,None);
         assert_eq!(p.observed_spot_sell_atoms_e12,Some(a("0.03").unwrap()));
         assert_eq!(p.remaining_owned_spot_atoms_e12,Some(a("0.059").unwrap()));
         assert_eq!(p.known_net_base_minus_short_atoms_e12,None);
         fs::remove_dir_all(d).unwrap();
         one(f,s);
     }
     #[test]
     fn both_partial_known_local_inventory_and_delta_not_verified_closed(){
         let f=Outcome::PartialExecution;let s=Outcome::PartialExecution;
         let (d,owner,_)=setup(C3Kind::ExactTargetShort);
         let p=inspect(&read(&owner),&evidence(f,s)).unwrap();
         assert_eq!(p.remaining_short_atoms_e12,Some(a("0.039").unwrap()));
         assert_eq!(p.remaining_owned_spot_atoms_e12,Some(a("0.059").unwrap()));
         assert_eq!(p.known_net_base_minus_short_atoms_e12,Some(a("0.02").unwrap() as i128));
         assert!(!p.may_send_retry_cancel_or_verify_closed);
         fs::remove_dir_all(d).unwrap();
         one(f,s);
     }
     #[test]
     fn both_unknown_are_not_confirmed_zero_or_ready_to_trade(){
         let u=Outcome::UnknownAcknowledgement;
         let (d,s,_)=setup(C3Kind::ExactTargetShort);
         let p=inspect(&read(&s),&evidence(u,u)).unwrap();
         assert_eq!(p.observed_futures_cover_atoms_e12,None);
         assert_eq!(p.observed_spot_sell_atoms_e12,None);
         assert_eq!(p.known_net_base_minus_short_atoms_e12,None);
         fs::remove_dir_all(d).unwrap();
         one(u,u);
     }
     #[test]
     fn foreign_owner_wrong_fee_bill_oversell_and_position_snapshot_denied(){
         let f=Outcome::PartialExecution;let s=Outcome::PartialExecution;
         for mode in 0..13{
             let (d,st,p)=setup(C3Kind::ExactTargetShort);
             let old=fs::read(&p).unwrap();let mut e=evidence(f,s);
             match mode{
                 0=>e.source_original_generation=98,
                 1=>e.source_v2_revision=3,
                 2=>e.futures_cover.client_id="new-futures-client".into(),
                 3=>e.spot_sell.intent_id="new-futures-intent".into(),
                 4=>e.futures_cover.fills[0].trade_id="88001".into(),
                 5=>e.futures_cover.fills[0].fee_currency="BNB".into(),
                 6=>e.futures_cover.fills[0].base_qty_atoms_e12=a("0.09").unwrap(),
                 7=>e.futures_cover.position_first.as_mut().unwrap().short_qty_atoms_e12=0,
                 8=>e.spot_sell.fills[0].bill_id="301".into(),
                 9=>e.spot_sell.fills[0].fee_currency="BNT".into(),
                 10=>e.spot_sell.fills[0].base_balance_after_e12=0,
                 11=>e.spot_sell.fills[0].base_qty_atoms_e12=a("0.089").unwrap(),
                 _=>e.futures_cover.fills[0].side="SELL".into(),
             }
             assert!(st.record_v2_emergency_close_review(4,e,FP::None).is_err(),"mode {mode}");
             assert_eq!(fs::read(&p).unwrap(),old);
             assert_eq!(read(&st).revision,4);
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn unknown_with_invented_zero_or_fake_terminal_and_censored_pages_denied(){
         for mode in 0..9{
             let (d,st,_)=setup(C3Kind::ExactTargetShort);
             let mut e=evidence(Outcome::UnknownAcknowledgement,Outcome::PartialExecution);
             match mode{
                 0=>e.futures_cover.order_id=Some("777".into()),
                 1=>e.futures_cover.pages_complete=true,
                 2=>e.futures_cover.position_first=Some(close().first_futures_flat),
                 3=>e.futures_cover.order_terminal_claimed=true,
                 4=>e.spot_sell.pages_complete=false,
                 5=>e.spot_sell.order_terminal_claimed=true,
                 6=>e.spot_sell.terminal_empty_page_seen=false,
                 7=>e.independent_rollback_floor_verified=true,
                 _=>e.may_retry_send_cancel_or_verify_closed=true,
             }
             assert!(st.record_v2_emergency_close_review(4,e,FP::None).is_err(),"mode {mode}");
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn partial_equal_target_duplicate_or_wrong_sequence_denied(){
         for mode in 0..7{
             let (d,st,_)=setup(C3Kind::ExactTargetShort);
             let mut e=evidence(Outcome::PartialExecution,Outcome::PartialExecution);
             match mode{
                 0=>e.futures_cover.fills[0].base_qty_atoms_e12=a("0.089").unwrap(),
                 1=>e.spot_sell.fills[0].base_qty_atoms_e12=a("0.089").unwrap(),
                 2=>e.futures_cover.fills[0].occurred_at_ms=1_780_000_004_500,
                 3=>e.spot_sell.fills[0].occurred_at_ms=1_780_000_002_900,
                 4=>e.futures_cover.position_first.as_mut().unwrap().captured_at_ms=1_780_000_002_000,
                 5=>e.futures_cover.position_second.as_mut().unwrap().captured_at_ms=1_780_000_003_400,
                 _=>e.spot_sell.fills[0].trade_id="9001".into(),
             }
             assert!(st.record_v2_emergency_close_review(4,e,FP::None).is_err(),"mode {mode}");
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn c3_partial_or_unknown_needs_different_recovery_gate(){
         for kind in [C3Kind::PartialShort,C3Kind::UnknownAcknowledgement]{
             let (d,st,_)=setup(kind);
             assert!(st.record_v2_emergency_close_review(4,
                 evidence(Outcome::UnknownAcknowledgement,Outcome::UnknownAcknowledgement),
                 FP::None).is_err());
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn emergency_and_normal_close_race_exactly_one_cas_wins(){
         let (d,owner,path)=setup(C3Kind::ExactTargetShort);
         let sync=Arc::new(Barrier::new(7));let mut threads=vec![];
         for n in 0..6{
             let p=path.clone();let b=sync.clone();
             threads.push(thread::spawn(move||{
                 let owner=NativeStateStore::open(&p).unwrap();b.wait();
                 if n%2==0{
                     owner.record_v2_emergency_close_review(4,
                         evidence(Outcome::PartialExecution,Outcome::UnknownAcknowledgement),
                         FP::None).is_ok()
                 }else{
                     owner.record_v2_four_leg_close_review(4,close(),FP::None).is_ok()
                 }
             }));
         }
         sync.wait();
         assert_eq!(threads.into_iter().map(|t|t.join().unwrap()).filter(|x|*x).count(),1);
         let result=read(&owner);
         assert_eq!(result.revision,5);
         assert_ne!(result.close_four_leg.is_some(),result.emergency_close.is_some());
         assert!(!result.production_authorized);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn interrupted_emergency_write_quarantines_and_denies_retry(){
         for fp in [FP::PartialWrite,FP::AfterFileSync]{
             let (d,st,path)=setup(C3Kind::ExactTargetShort);
             let old=fs::read(&path).unwrap();
             let e=evidence(Outcome::PartialExecution,Outcome::UnknownAcknowledgement);
             assert!(st.record_v2_emergency_close_review(4,e.clone(),fp).is_err());
             assert!(pending_path(&path).exists());
             assert_eq!(fs::read(&path).unwrap(),old);
             assert!(st.load_versioned_review().is_err());
             assert!(st.record_v2_emergency_close_review(4,e,FP::None).is_err());
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn post_rename_unsynced_ack_does_not_authorize_mutations(){
         let (d,st,path)=setup(C3Kind::ExactTargetShort);
         assert!(st.record_v2_emergency_close_review(4,
             evidence(Outcome::UnknownAcknowledgement,Outcome::PartialExecution),
             FP::AfterRename).is_err());
         let reopened=NativeStateStore::open(&path).unwrap();
         let state=read(&reopened);
         assert_eq!(state.phase,Phase::EmergencyCloseRecorded);
         assert_eq!(state.revision,5);
         assert!(!state.may_send_or_cancel_or_verify_closed);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn local_rollback_is_still_exploitable_without_external_floor(){
         let (d,st,path)=setup(C3Kind::ExactTargetShort);
         let old=fs::read(&path).unwrap();
         st.record_v2_emergency_close_review(4,
             evidence(Outcome::PartialExecution,Outcome::UnknownAcknowledgement),
             FP::None).unwrap();
         assert_eq!(read(&st).revision,5);
         fs::write(&path,old).unwrap();
         assert_eq!(read(&st).revision,4);
         fs::remove_dir_all(d).unwrap();
     }
 }

 // C3 opening-side recovery: a later mock history may NOT silently erase
 // originally known fills or authorize retry after an uncertain ACK.
 mod v2_c3_opening_recovery_owner_tests {
     use super::*;
     use super::v2_four_leg_native_close_tests::{setup,read,close};
     use super::v2_c3_native_owner_tests::observation;
     use spotarb_native_market_core::{
         authoritative_state::NativeStateStore,
         native_trade2f3_v2_versioned_store::{V2ReviewPhase as Phase,V2ReviewFailpoint as FP,pending_path},
         native_trade2f3_v2_c3_futures_evidence::{
             V2FuturesObservationKind as Kind,V2FuturesTrade as Trade,
             V2FuturesPositionSnapshot as Snapshot,
         },
         native_trade2f3_v2_c3_opening_recovery::{
             V2C3OpeningRecoveryEvidence as Recovery,V2OpeningRecoveryKind as RKind,
             inspect_v2_c3_opening_recovery as inspect,
         },
     };
     use std::{fs,sync::{Arc,Barrier},thread};
     fn fixture(start:Kind,after:RKind)->Recovery{
         let original=observation(start);
         let mut trades=original.rows;
         let (order,client,first,second,pages,empty)=match after{
             RKind::StillUnknown=>(None,None,None,None,false,false),
             _=>{
                 if start==Kind::UnknownAcknowledgement {
                     trades.push(Trade{
                         trade_id:"88001".into(),order_id:"777".into(),
                         symbol:"BNTUSDT".into(),side:"SELL".into(),position_side:"BOTH".into(),
                         occurred_at_ms:1_780_000_001_500,
                         base_qty_atoms_e12:a("0.05").unwrap(),
                         price_quote_atoms_e12:a("100").unwrap(),
                         fee_currency:"USDT".into(),
                         signed_fee_quote_atoms_e12:-(a("0.002").unwrap() as i128),
                         realized_pnl_quote_atoms_e12:0,
                     });
                 }
                 if after==RKind::ObservedExact {
                     trades.push(Trade{
                         trade_id:"88003".into(),order_id:"777".into(),
                         symbol:"BNTUSDT".into(),side:"SELL".into(),position_side:"BOTH".into(),
                         occurred_at_ms:1_780_000_002_300,
                         base_qty_atoms_e12:a("0.039").unwrap(),
                         price_quote_atoms_e12:a("100").unwrap(),
                         fee_currency:"USDT".into(),
                         signed_fee_quote_atoms_e12:-(a("0.001").unwrap() as i128),
                         realized_pnl_quote_atoms_e12:0,
                     });
                 }
                 let qty=if after==RKind::ObservedExact {
                     a("0.089").unwrap()
                 }else{a("0.05").unwrap()};
                 let first=Some(Snapshot{
                     captured_at_ms:1_780_000_002_700,
                     futures_symbol:"BNTUSDT".into(),
                     short_qty_atoms_e12:qty,claimed_prior_short_atoms_e12:0,
                 });
                 let second=Some(Snapshot{
                     captured_at_ms:1_780_000_002_800,
                     futures_symbol:"BNTUSDT".into(),
                     short_qty_atoms_e12:qty,claimed_prior_short_atoms_e12:0,
                 });
                 (Some("777".into()),Some("new-futures-client".into()),first,second,true,true)
             }
         };
         if after==RKind::StillUnknown{trades.clear();}
         Recovery{
             original_owner_generation:99,parent_v2_revision:4,
             original_intent_id:"new-futures-intent".into(),
             original_client_id:"new-futures-client".into(),
             futures_symbol:"BNTUSDT".into(),
             observed_order_id:order,recovered_client_id:client,
             kind:after,queried_from_ms:1_780_000_000_000,
             captured_at_ms:1_780_000_003_000,
             bounded_pages_complete:pages,terminal_empty_page_seen:empty,
             reported_order_terminal:after==RKind::ObservedExact,
             fills:trades,first_position:first,second_position:second,
             account_wide_exchange_truth_authenticated:false,
             external_antirollback_proven:false,
             may_send_retry_cancel_or_promote_hedged:false,
         }
     }
     #[test]
     fn original_unknown_stays_unknown_and_never_zeros_exposure(){
         let (dir,owner,path)=setup(Kind::UnknownAcknowledgement);
         let witness=fixture(Kind::UnknownAcknowledgement,RKind::StillUnknown);
         let review=inspect(&read(&owner),&witness).unwrap();
         assert_eq!(review.original_known_short_floor_atoms_e12,None);
         assert_eq!(review.observed_short_atoms_e12,None);
         assert_eq!(review.remaining_unhedged_atoms_e12,None);
         assert!(!review.may_send_retry_cancel_or_promote_hedged);
         let saved=owner.record_v2_c3_opening_recovery_review(4,witness.clone(),FP::None).unwrap();
         assert_eq!(saved.revision,5);
         assert_eq!(saved.phase,Phase::OpeningRecoveryRecorded);
         assert_eq!(saved.opening_recovery,Some(witness));
         assert!(!saved.production_authorized);
         assert_eq!(read(&NativeStateStore::open(path).unwrap()),saved);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn originally_partial_lost_response_retains_immutable_known_floor(){
         let (dir,owner,_)=setup(Kind::PartialShort);
         let witness=fixture(Kind::PartialShort,RKind::StillUnknown);
         let r=inspect(&read(&owner),&witness).unwrap();
         assert_eq!(r.original_known_short_floor_atoms_e12,Some(a("0.05").unwrap()));
         assert_eq!(r.observed_short_atoms_e12,None);
         assert_eq!(r.remaining_unhedged_atoms_e12,None);
         assert!(!r.locally_exact_short_shape);
         owner.record_v2_c3_opening_recovery_review(4,witness,FP::None).unwrap();
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn partial_to_locally_exact_consumes_net_target_but_not_hedged(){
         let (dir,owner,path)=setup(Kind::PartialShort);
         let witness=fixture(Kind::PartialShort,RKind::ObservedExact);
         let r=inspect(&read(&owner),&witness).unwrap();
         assert_eq!(r.original_known_short_floor_atoms_e12,Some(a("0.05").unwrap()));
         assert_eq!(r.observed_short_atoms_e12,Some(a("0.089").unwrap()));
         assert_eq!(r.remaining_unhedged_atoms_e12,Some(0));
         assert!(r.locally_exact_short_shape);
         assert!(!r.exchange_authoritative);
         assert!(!r.may_send_retry_cancel_or_promote_hedged);
         let saved=owner.record_v2_c3_opening_recovery_review(4,witness,FP::None).unwrap();
         assert_eq!(saved.phase,Phase::OpeningRecoveryRecorded);
         assert!(saved.close_four_leg.is_none()&&saved.emergency_close.is_none());
         assert!(owner.record_v2_four_leg_close_review(5,close(),FP::None).is_err());
         let terminal=owner.advance_v2_review(5,Phase::RecoveryOnly,FP::None).unwrap();
         assert_eq!(terminal.revision,6);
         assert_eq!(read(&NativeStateStore::open(path).unwrap()),terminal);
         assert!(!terminal.may_send_or_cancel_or_verify_closed);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn unknown_to_locally_observed_partial_then_exact_shapes(){
         for kind in [RKind::ObservedPartial,RKind::ObservedExact]{
             let (dir,owner,_)=setup(Kind::UnknownAcknowledgement);
             let r=inspect(&read(&owner),&fixture(Kind::UnknownAcknowledgement,kind)).unwrap();
             assert_eq!(r.original_known_short_floor_atoms_e12,None);
             assert_eq!(r.observed_short_atoms_e12,Some(
                 if kind==RKind::ObservedExact{a("0.089").unwrap()}else{a("0.05").unwrap()}));
             assert_eq!(r.locally_exact_short_shape,kind==RKind::ObservedExact);
             assert!(!r.exchange_authoritative);
             owner.record_v2_c3_opening_recovery_review(4,
                 fixture(Kind::UnknownAcknowledgement,kind),FP::None).unwrap();
             fs::remove_dir_all(dir).unwrap();
         }
     }
     #[test]
     fn original_already_exact_rejects_attempt_to_replace_original_short(){
         let (dir,owner,path)=setup(Kind::ExactTargetShort);
         let old=fs::read(&path).unwrap();
         assert!(owner.record_v2_c3_opening_recovery_review(4,
             fixture(Kind::PartialShort,RKind::ObservedExact),FP::None).is_err());
         assert_eq!(fs::read(&path).unwrap(),old);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn wrong_original_trade_duplicate_overhedge_fees_and_removed_known_trade_denied(){
         for mode in 0..12 {
             let (dir,owner,path)=setup(Kind::PartialShort);
             let mut w=fixture(Kind::PartialShort,RKind::ObservedExact);
             match mode{
                 0=>w.original_owner_generation=98,
                 1=>w.parent_v2_revision=3,
                 2=>w.original_client_id="foreign".into(),
                 3=>w.recovered_client_id=Some("foreign".into()),
                 4=>w.observed_order_id=Some("778".into()),
                 5=>w.fills[0].fee_currency="BNB".into(),
                 6=>w.fills[0].base_qty_atoms_e12=a("0.051").unwrap(),
                 7=>{w.fills.remove(0);},
                 8=>{let clone=w.fills[0].clone();w.fills.push(clone);},
                 9=>w.fills[1].base_qty_atoms_e12=a("0.04").unwrap(),
                 10=>w.fills[0].trade_id="88004".into(),
                 _=>w.fills[1].position_side="SHORT".into(),
             }
             let old=fs::read(&path).unwrap();
             assert!(owner.record_v2_c3_opening_recovery_review(4,w,FP::None).is_err(),
                 "case {mode}");
             assert_eq!(fs::read(&path).unwrap(),old);
             assert_eq!(read(&owner).revision,4);
             fs::remove_dir_all(dir).unwrap();
         }
     }
     #[test]
     fn claim_zero_or_unsupported_terminal_when_unknown_or_partial_rejected(){
         for mode in 0..9{
             let (dir,owner,_)=setup(Kind::UnknownAcknowledgement);
             let mut w=fixture(Kind::UnknownAcknowledgement,RKind::StillUnknown);
             match mode{
                 0=>w.observed_order_id=Some("777".into()),
                 1=>w.recovered_client_id=Some("new-futures-client".into()),
                 2=>w.bounded_pages_complete=true,
                 3=>w.terminal_empty_page_seen=true,
                 4=>w.reported_order_terminal=true,
                 5=>w.first_position=Some(Snapshot{
                     captured_at_ms:1_780_000_002_700,futures_symbol:"BNTUSDT".into(),
                     short_qty_atoms_e12:0,claimed_prior_short_atoms_e12:0}),
                 6=>w.account_wide_exchange_truth_authenticated=true,
                 7=>w.external_antirollback_proven=true,
                 _=>w.may_send_retry_cancel_or_promote_hedged=true,
             }
             assert!(owner.record_v2_c3_opening_recovery_review(4,w,FP::None).is_err());
             fs::remove_dir_all(dir).unwrap();
         }
     }
     #[test]
     fn order_status_page_gap_snapshot_time_owner_and_fee_sign_denied(){
         for mode in 0..10{
             let (dir,owner,_)=setup(Kind::PartialShort);
             let mut w=fixture(Kind::PartialShort,RKind::ObservedExact);
             match mode{
                 0=>w.bounded_pages_complete=false,
                 1=>w.terminal_empty_page_seen=false,
                 2=>w.first_position.as_mut().unwrap().short_qty_atoms_e12=a("0.05").unwrap(),
                 3=>w.second_position.as_mut().unwrap().captured_at_ms=1_780_000_002_700,
                 4=>w.first_position.as_mut().unwrap().captured_at_ms=1_780_000_002_000,
                 5=>w.fills[1].signed_fee_quote_atoms_e12=1,
                 6=>w.fills[1].side="BUY".into(),
                 7=>w.fills[1].order_id="778".into(),
                 8=>w.kind=RKind::ObservedPartial,
                 _=>w.reported_order_terminal=false,
             }
             assert!(owner.record_v2_c3_opening_recovery_review(4,w,FP::None).is_err(),
                 "case {mode}");
             fs::remove_dir_all(dir).unwrap();
         }
     }
     #[test]
     fn six_concurrent_original_owner_cas_writers_exactly_one_succeeds(){
         let (dir,owner,path)=setup(Kind::PartialShort);
         let sync=Arc::new(Barrier::new(7));let mut handles=Vec::new();
         for n in 0..6{
             let p=path.clone();let sync=sync.clone();
             handles.push(thread::spawn(move||{
                 let s=NativeStateStore::open(p).unwrap();
                 let k=if n%2==0{RKind::StillUnknown}else{RKind::ObservedExact};
                 let w=fixture(Kind::PartialShort,k);
                 sync.wait();
                 s.record_v2_c3_opening_recovery_review(4,w,FP::None).is_ok()
             }));
         }
         sync.wait();
         assert_eq!(handles.into_iter().map(|h|h.join().unwrap()).filter(|x|*x).count(),1);
         assert_eq!(read(&owner).revision,5);
         assert!(!read(&owner).production_authorized);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn incomplete_write_quarantines_recovery_and_no_blind_retry(){
         for fp in [FP::PartialWrite,FP::AfterFileSync]{
             let (dir,owner,path)=setup(Kind::UnknownAcknowledgement);
             let before=fs::read(&path).unwrap();
             let witness=fixture(Kind::UnknownAcknowledgement,RKind::ObservedPartial);
             assert!(owner.record_v2_c3_opening_recovery_review(4,witness.clone(),fp).is_err());
             assert_eq!(fs::read(&path).unwrap(),before);
             assert!(pending_path(&path).exists());
             assert!(owner.load_versioned_review().is_err());
             assert!(owner.record_v2_c3_opening_recovery_review(4,witness,FP::None).is_err());
             fs::remove_dir_all(dir).unwrap();
         }
     }
     #[test]
     fn crash_after_rename_before_parent_fsync_does_not_return_ack(){
         let (dir,owner,path)=setup(Kind::PartialShort);
         let witness=fixture(Kind::PartialShort,RKind::ObservedExact);
         assert!(owner.record_v2_c3_opening_recovery_review(4,witness,FP::AfterRename).is_err());
         let cold=read(&NativeStateStore::open(path).unwrap());
         assert_eq!(cold.revision,5);
         assert_eq!(cold.phase,Phase::OpeningRecoveryRecorded);
         assert!(!cold.may_send_or_cancel_or_verify_closed);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn intact_snapshot_rollback_demonstrably_unprotected_without_external_floor(){
         let (dir,owner,path)=setup(Kind::UnknownAcknowledgement);
         let prior=fs::read(&path).unwrap();
         owner.record_v2_c3_opening_recovery_review(4,
             fixture(Kind::UnknownAcknowledgement,RKind::ObservedExact),FP::None).unwrap();
         assert_eq!(read(&owner).revision,5);
         fs::write(&path,prior).unwrap();
         assert_eq!(read(&owner).revision,4);
         fs::remove_dir_all(dir).unwrap();
     }
 }

 // Astra independent antirollback contract: first prove what a co-located
 // append-only journal can/cannot enforce WITHOUT an external trust anchor.
 mod v2_revision_journal_floor_tests {
     use super::*;
     use super::v2_owner_genesis_review_tests::example;
     use super::v2_c3_native_owner_tests::observation;
     use spotarb_native_market_core::{
         authoritative_state::{NativeStateStore,NativeVersionedReview},
         native_trade2f3_v2_versioned_store::{V2ReviewPhase as Phase,V2ReviewFailpoint as FP},
         native_trade2f3_v2_c3_futures_evidence::V2FuturesObservationKind as Kind,
         native_trade2f3_v2_revision_journal::{
             journal_path,read_local_journal,V2JournalFailpoint as JP,
             V2CallerHeldFloor as Floor,review_owner_against_local_journal,
         },
     };
     use std::{fs,path::PathBuf,sync::{Arc,Barrier,atomic::{AtomicU64,Ordering}},thread};
     static NEXT:AtomicU64=AtomicU64::new(0);
     fn setup()->(PathBuf,NativeStateStore,PathBuf){
         let dir=std::env::temp_dir().join(format!("astra_v2_floor_{}_{}",
             std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
         fs::create_dir(&dir).unwrap();
         let path=dir.join("owner.json");
         let st=NativeStateStore::open(&path).unwrap();
         st.create_v2_review(example(true),FP::None).unwrap();
         (dir,st,path)
     }
     fn current(st:&NativeStateStore)->spotarb_native_market_core::native_trade2f3_v2_versioned_store::V2ReviewState{
         match st.load_versioned_review().unwrap().unwrap(){
             NativeVersionedReview::FeeAwareV2(v)=>v,_=>panic!("missing V2")
         }
     }
     pub(super) fn setup_journal_rev4()->(PathBuf,NativeStateStore,PathBuf,Vec<Vec<u8>>,Vec<Vec<u8>>){
         let (d,s,p)=setup();
         let mut owners=Vec::new();
         let mut logs=Vec::new();
         for rev in 1..=4 {
             if rev==2{s.advance_v2_review(1,Phase::BillsReviewed,FP::None).unwrap();}
             if rev==3{s.advance_v2_review(2,Phase::HedgeSizeReviewed,FP::None).unwrap();}
             if rev==4{s.record_v2_c3_review(3,observation(Kind::ExactTargetShort),FP::None).unwrap();}
             s.append_v2_revision_journal_review(JP::None).unwrap();
             owners.push(fs::read(&p).unwrap());
             logs.push(fs::read(journal_path(&p)).unwrap());
         }
         (d,s,p,owners,logs)
     }
     #[test]
     fn local_v2_paired_inventory_requires_owner_and_revision_journal(){
         use spotarb_native_market_core::
             native_paper_v2_close_source_archive_review::
             {scan_local_v2_owner_journal_pairs as scan,
              scan_paired_v2_against_claimed_owner_floors as pinned,
              ClaimedLocalOwnerAnchor,
              export_local_v2_floor_manifest as export_floor,
              inspect_portable_v2_floor_successor as check_next,
              verify_local_v2_portable_floor_manifest as verify_floor,
              PortableOwnerFloorManifest};
         let (folder,store,owner_path,owners,logs)=setup_journal_rev4();
         let root=folder.join("paired_inventory");
         fs::create_dir(&root).unwrap();
         let dest=root.join("owner.json");
         let journal=journal_path(&dest);
         fs::copy(&owner_path,&dest).unwrap();
         fs::copy(journal_path(&owner_path),&journal).unwrap();
         let expected_generation=current(&store).genesis.original_owner_generation;
         let one=scan(&root).unwrap();
         assert_eq!(one.unresolved_owner_generations,vec![expected_generation]);
         assert!(one.locally_reviewed_four_leg_generations.is_empty());
         assert!(!one.source_account_authenticated);
         assert!(!one.actual_closed_trades_proven);
         assert!(!one.full_owner_history_proven);
         assert!(!one.may_trade_real);
         let revision4=read_local_journal(&owner_path).unwrap().pop().unwrap();
         let floor4=Floor{
             original_owner_generation:expected_generation,
             minimum_revision:4,
             exact_entry_sha256:revision4.entry_sha256.clone(),
         };
         let anchors4=[ClaimedLocalOwnerAnchor{
             owner_filename:"owner.json",floor:&floor4,
         }];
         assert_eq!(pinned(&root,&anchors4).unwrap().examined_owner_files,1);
         let exported4=export_floor(&root).unwrap();
         let pin4=exported4.manifest_sha256.clone();
         let json=serde_json::to_vec(&exported4).unwrap();
         let portable4:PortableOwnerFloorManifest=
             serde_json::from_slice(&json).unwrap();
         assert_eq!(exported4,portable4);
         assert_eq!(verify_floor(&root,&portable4,&pin4).unwrap().examined_owner_files,1);
         assert!(verify_floor(&root,&portable4,"").is_err(),
             "never silently trust an unpinned manifest");
         assert!(pinned(&root,&[]).is_err(),"omitted whole population");
         let foreign=[ClaimedLocalOwnerAnchor{
             owner_filename:"not-found.json",floor:&floor4,
         }];
         assert!(pinned(&root,&foreign).is_err(),"unlisted owner path");
         // A real locally appended revision-5 close changes both paired files.
         store.record_v2_four_leg_close_review(
             4,super::v2_four_leg_native_close_tests::close(),FP::None).unwrap();
         store.append_v2_revision_journal_review(JP::None).unwrap();
         fs::copy(&owner_path,&dest).unwrap();
         fs::copy(journal_path(&owner_path),&journal).unwrap();
         let reviewed=scan(&root).unwrap();
         assert_eq!(reviewed.locally_reviewed_four_leg_generations,
             vec![expected_generation]);
         assert!(reviewed.unresolved_owner_generations.is_empty());
         assert!(!reviewed.actual_closed_trades_proven);
         assert!(pinned(&root,&anchors4).is_ok(),
             "a historical claimed ancestor permits later journal revisions");
         let revision5=read_local_journal(&owner_path).unwrap().pop().unwrap();
         let floor5=Floor{
             original_owner_generation:expected_generation,
             minimum_revision:5,
             exact_entry_sha256:revision5.entry_sha256.clone(),
         };
         let anchors5=[ClaimedLocalOwnerAnchor{
             owner_filename:"owner.json",floor:&floor5,
         }];
         assert!(pinned(&root,&anchors5).is_ok());
         let exported5=export_floor(&root).unwrap();
         let pin5=exported5.manifest_sha256.clone();
         assert_ne!(pin4,pin5,"new revision must change portable pin");
         assert!(check_next(&root,&portable4,&pin4,&exported5,&pin5).is_ok(),
             "rev4 to rev5 retains old journal ancestry");
         assert!(check_next(&root,&exported5,&pin5,&exported5,&pin5).is_err(),
             "unchanged version is not advancement");
         assert!(check_next(&root,&exported5,&pin5,&portable4,&pin4).is_err(),
             "candidate may not retreat to rev4");
         assert!(verify_floor(&root,&exported5,&pin5).is_ok());
         let cli=env!("CARGO_BIN_EXE_native_v2_floor_offline");
         let exported=std::process::Command::new(cli)
             .arg("export").arg(&root).output().unwrap();
         assert!(exported.status.success(),"CLI export denied");
         let wire:PortableOwnerFloorManifest=
             serde_json::from_slice(&exported.stdout).unwrap();
         assert_eq!(wire,exported5);
         let copied=root.parent().unwrap().join("operator_copy.json");
         fs::write(&copied,&exported.stdout).unwrap();
         let verified=std::process::Command::new(cli)
             .arg("verify").arg(&root).arg(&copied).arg(&pin5)
             .output().unwrap();
         assert!(verified.status.success(),"CLI verify denied valid manifest");
         let previous_copy=root.parent().unwrap().join("held_old.json");
         fs::write(&previous_copy,serde_json::to_vec(&portable4).unwrap()).unwrap();
         let cli_successor=std::process::Command::new(cli)
             .arg("check-next").arg(&root).arg(&previous_copy).arg(&pin4)
             .arg(&copied).arg(&pin5).output().unwrap();
         assert!(cli_successor.status.success(),"CLI successor denied valid growth");
         let unchanged_cli=std::process::Command::new(cli)
             .arg("check-next").arg(&root).arg(&copied).arg(&pin5)
             .arg(&copied).arg(&pin5).output().unwrap();
         assert!(!unchanged_cli.status.success(),"duplicate pin promoted");
         let back_cli=std::process::Command::new(cli)
             .arg("check-next").arg(&root).arg(&copied).arg(&pin5)
             .arg(&previous_copy).arg(&pin4).output().unwrap();
         assert!(!back_cli.status.success(),"old owner revision promoted");
         let denied=std::process::Command::new(cli)
             .arg("verify").arg(&root).arg(&copied).arg(&pin4)
             .output().unwrap();
         assert!(!denied.status.success(),"wrong operator pin accepted");
         assert!(verify_floor(&root,&exported5,&pin4).is_err());
         assert!(verify_floor(&root,&portable4,&pin4).is_ok(),
             "older floor remains valid ancestor");
         let mut tampered=exported5.clone();
         tampered.owners[0].floor.exact_entry_sha256="f".repeat(64);
         assert!(verify_floor(&root,&tampered,&pin5).is_err());
         let bad_floor=Floor{
             exact_entry_sha256:"a".repeat(64),..floor5.clone()
         };
         let bad=[ClaimedLocalOwnerAnchor{
             owner_filename:"owner.json",floor:&bad_floor,
         }];
         assert!(pinned(&root,&bad).is_err(),"wrong claimed SHA");
         // Same-host owner + journal can be rolled back TOGETHER:
         // successful local validation must never assert true completeness.
         fs::write(&dest,&owners[2]).unwrap();
         fs::write(&journal,&logs[2]).unwrap();
         let rolled_back=scan(&root).unwrap();
         assert_eq!(rolled_back.unresolved_owner_generations,
             vec![expected_generation]);
         assert!(!rolled_back.full_owner_history_proven);
         assert!(pinned(&root,&anchors5).is_err(),
             "the previously recorded fifth revision detects joint rollback");
         assert!(verify_floor(&root,&exported5,&pin5).is_err(),
             "portable floor5 detects locally consistent joint rollback");
         let rolled_cli=std::process::Command::new(cli)
             .arg("verify").arg(&root).arg(&copied).arg(&pin5)
             .output().unwrap();
         assert!(!rolled_cli.status.success(),
             "CLI must refuse joint rollback against the held floor5");
         fs::write(&dest,&owners[3]).unwrap();
         fs::write(&journal,&logs[3]).unwrap();
         assert!(scan(&root).is_ok());
         assert!(pinned(&root,&anchors4).is_ok(),
             "the locally claimed rev4 is still a valid ancestor");
         assert!(pinned(&root,&anchors5).is_err(),
             "rev5 anchor detects coordinated rev4 rollback");
         // A mixed owner/journal revision must be refused.
         fs::copy(&owner_path,&dest).unwrap();
         assert!(scan(&root).is_err());
         fs::copy(journal_path(&owner_path),&journal).unwrap();
         assert!(scan(&root).is_ok());
         fs::remove_file(&journal).unwrap();
         assert!(scan(&root).is_err(),"missing journal denied");
         fs::copy(journal_path(&owner_path),&journal).unwrap();
         fs::remove_file(&dest).unwrap();
         assert!(scan(&root).is_err(),"orphan journal denied");
         fs::copy(&owner_path,&dest).unwrap();
         let second=root.join("unpaired.json");
         fs::copy(&owner_path,&second).unwrap();
         assert!(scan(&root).is_err(),"missing second owner's journal denied");
         fs::remove_file(second).unwrap();
         assert!(scan(&root).is_ok());
         let second=root.join("second.json");
         let second_log=journal_path(&second);
         fs::copy(&dest,&second).unwrap();
         fs::copy(&journal,&second_log).unwrap();
         assert!(scan(&root).is_err(),"duplicate owner generation denied");
         fs::remove_file(second).unwrap();
         fs::remove_file(second_log).unwrap();
         fs::write(&journal,b"truncated\n").unwrap();
         assert!(scan(&root).is_err(),"corrupt journal denied");
         fs::copy(journal_path(&owner_path),&journal).unwrap();
         assert!(scan(&root).is_ok());
         fs::remove_dir_all(folder).unwrap();
     }
     #[test]
     fn local_pair_witness_tip_uses_full_v2_and_paper_cold_journals(){
         use openssl::{pkey::PKey,sign::Signer};
         use spotarb_native_market_core::{
             native_paper_owner_transaction_review::PaperOwner,
             native_paper_v2_pair_witness_review::{
                 PairTip,PairSignedLatestPayload,PairSignedLatest,PairPinnedChallenge,
                 PairWitnessError,PAIR_REPLY_DOMAIN,pair_latest_message,
                 verify_signed_pair_latest,
             },
         };
         use super::v2_four_leg_native_close_tests::{
             build_v2_compatible_paper,paper_owner_with_gate,close,
         };
         let (folder,store,v2_path,_,_)=setup_journal_rev4();
         let local_v2=current(&store);
         let validated=store.verify_v2_revision_journal_review(None).unwrap();
         assert!(validated.local_chain_consistent);
         assert!(!validated.external_independence_authenticated);
         let journal=read_local_journal(&v2_path).unwrap();
         let original_tip=journal.last().unwrap();
         let gate=build_v2_compatible_paper(&local_v2,&close(),false);
         let mut paper=paper_owner_with_gate(&folder,gate);
         let cold=PaperOwner::inspect_recovery(&paper.journal).unwrap().unwrap();
         let pair=PairTip::from_validated_local_tips(original_tip,&cold).unwrap();
         assert_eq!(pair.owner_generation,99);
         assert_eq!(pair.v2_revision,4);
         assert_eq!(pair.paper_sequence,2);
         let key=PKey::generate_ed25519().unwrap();
         let nonce="a".repeat(64);
         let payload=PairSignedLatestPayload{
            protocol:1,domain:PAIR_REPLY_DOMAIN.into(),
            witness_id:"offline_pair_fixture".into(),
            request_nonce_hex:nonce.clone(),
            witness_pair_revision:1,latest:pair.clone(),
         };
         let mut signer=Signer::new_without_digest(&key).unwrap();
         let sig=signer.sign_oneshot_to_vec(&pair_latest_message(&payload).unwrap()).unwrap();
         let signed=PairSignedLatest{
             payload,signature_ed25519_hex:sig.iter().map(|b|format!("{b:02x}")).collect(),
         };
         let pin=PairPinnedChallenge{
             trusted_witness_id:"offline_pair_fixture".into(),
             trusted_public_key_hex:key.raw_public_key().unwrap().iter()
                 .map(|b|format!("{b:02x}")).collect(),
             freshly_requested_nonce_hex:nonce,
         };
         let matched=verify_signed_pair_latest(Some(&signed),&pin,&pair).unwrap();
         assert!(matched.same_local_pair);
         assert!(!matched.remote_currentness_proven);
         assert!(!matched.may_resume_or_trade);
         assert!(!matched.independently_verified_closed);
         // An additional PAPER-only CAS produces a different local pair.
         // Signed old response must fail until exact new pair is attested.
         paper.transact(|_,_|Ok(())).unwrap();
         let recent=PaperOwner::inspect_recovery(&paper.journal).unwrap().unwrap();
         let now=PairTip::from_validated_local_tips(original_tip,&recent).unwrap();
         assert_eq!(now.paper_sequence,3);
         assert_eq!(verify_signed_pair_latest(Some(&signed),&pin,&now),
             Err(PairWitnessError::WitnessBehindLocal));
         // Old journal data alone cannot resurrect permission after restart.
         assert!(!recent.may_resume_paper&&!recent.may_trade_real);
         fs::remove_dir_all(folder).unwrap();
     }
     #[test]
     fn append_each_v2_cas_revision_cold_reopen_verified_but_not_independent(){
         let (d,s,p,_,_)=setup_journal_rev4();
         let snapshot=read_local_journal(&p).unwrap();
         assert_eq!(snapshot.len(),4);
         for (i,r) in snapshot.iter().enumerate(){assert_eq!(r.revision,(i+1) as u64);}
         let latest=snapshot.last().unwrap().clone();
         let supplied=Floor{
             original_owner_generation:99,
             minimum_revision:latest.revision,
             exact_entry_sha256:latest.entry_sha256,
         };
         let cold=NativeStateStore::open(&p).unwrap();
         let r=cold.verify_v2_revision_journal_review(Some(&supplied)).unwrap();
         assert_eq!(r.current_revision,4);
         assert_eq!(r.journal_entries,4);
         assert!(r.local_chain_consistent);
         assert!(r.supplied_floor_matches);
         assert!(!r.external_independence_authenticated);
         assert!(!r.may_send_retry_cancel_verify_closed);
         assert!(!current(&cold).production_authorized);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn missing_journal_always_denies_review_only_preflight(){
         let (d,s,_)=setup();
         assert!(s.verify_v2_revision_journal_review(None).is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn owner_rolled_back_but_local_journal_kept_is_detected(){
         let (d,s,p,owner,_)=setup_journal_rev4();
         fs::write(&p,&owner[2]).unwrap();
         assert_eq!(current(&s).revision,3); // Original local V2 checksum alone accepts it.
         assert!(s.verify_v2_revision_journal_review(None).is_err());
         assert!(s.append_v2_revision_journal_review(JP::None).is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn local_journal_rolled_back_but_owner_kept_is_detected(){
         let (d,s,p,_owner,logs)=setup_journal_rev4();
         fs::write(journal_path(&p),&logs[2]).unwrap();
         assert_eq!(current(&s).revision,4);
         assert!(s.verify_v2_revision_journal_review(None).is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn coherent_dual_rollback_bypasses_local_only_but_caller_held_floor_detects(){
         let (d,s,p,owners,logs)=setup_journal_rev4();
         let tip=read_local_journal(&p).unwrap().last().unwrap().clone();
         let preserved_elsewhere=Floor{original_owner_generation:99,
             minimum_revision:4,exact_entry_sha256:tip.entry_sha256};
         fs::write(&p,&owners[2]).unwrap();
         fs::write(journal_path(&p),&logs[2]).unwrap();
         assert_eq!(current(&s).revision,3);
         let local=s.verify_v2_revision_journal_review(None).unwrap();
         assert!(local.local_chain_consistent);
         assert!(!local.supplied_floor_matches);
         assert!(!local.external_independence_authenticated);
         assert!(s.verify_v2_revision_journal_review(Some(&preserved_elsewhere)).is_err());
         // We cannot authenticate that a floor was really maintained elsewhere.
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn forgery_missing_revised_hash_future_and_wrong_generation_floor_denied(){
         for mode in 0..5 {
             let (d,s,p,_,_)=setup_journal_rev4();
             let e=read_local_journal(&p).unwrap().last().unwrap().clone();
             let mut floor=Floor{original_owner_generation:99,
                 minimum_revision:4,exact_entry_sha256:e.entry_sha256};
             match mode {
                 0=>floor.original_owner_generation=98,
                 1=>floor.minimum_revision=5,
                 2=>floor.minimum_revision=0,
                 3=>floor.exact_entry_sha256="0".repeat(64),
                 _=>floor.exact_entry_sha256="BAD".into(),
             }
             assert!(s.verify_v2_revision_journal_review(Some(&floor)).is_err(),"mode {mode}");
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn truncated_tampered_reordered_unterminated_and_extra_lines_denied(){
         for mode in 0..5{
             let (d,s,p,_,_)=setup_journal_rev4();
             let mut data=fs::read(journal_path(&p)).unwrap();
             match mode{
                 0=>{data.pop();},
                 1=>{data.truncate(data.len()/2);},
                 2=>{
                     let idx=data.iter().position(|x|*x==b'4').unwrap();
                     data[idx]=b'5';
                 }
                 3=>{
                     let parts=data.split(|b|*b==b'\n').filter(|x|!x.is_empty())
                         .map(|x|x.to_vec()).collect::<Vec<_>>();
                     data=Vec::new();
                     for x in [parts[0].clone(),parts[2].clone(),parts[1].clone(),parts[3].clone()]{
                         data.extend_from_slice(&x);data.push(b'\n');
                     }
                 }
                 _=>{data.extend_from_slice(b"{}\n");}
             }
             fs::write(journal_path(&p),data).unwrap();
             assert!(s.verify_v2_revision_journal_review(None).is_err(),"mode {mode}");
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn skipped_owner_revision_refuses_synthetic_journal_backfill(){
         let (d,s,_p)=setup();
         s.append_v2_revision_journal_review(JP::None).unwrap();
         s.advance_v2_review(1,Phase::BillsReviewed,FP::None).unwrap();
         s.advance_v2_review(2,Phase::HedgeSizeReviewed,FP::None).unwrap();
         assert!(s.append_v2_revision_journal_review(JP::None).is_err());
         assert!(s.verify_v2_revision_journal_review(None).is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn duplicate_journal_append_same_revision_never_acks_again(){
         let (d,s,p,_,_)=setup_journal_rev4();
         let before=fs::read(journal_path(&p)).unwrap();
         assert!(s.append_v2_revision_journal_review(JP::None).is_err());
         assert_eq!(fs::read(journal_path(&p)).unwrap(),before);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn torn_journal_tail_quarantines_read_and_blocks_append(){
         let (d,s,p)=setup();
         assert!(s.append_v2_revision_journal_review(JP::PartialTail).is_err());
         assert!(journal_path(&p).exists());
         assert!(s.verify_v2_revision_journal_review(None).is_err());
         assert!(s.append_v2_revision_journal_review(JP::None).is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn after_file_fsync_lost_ack_does_not_silently_append_duplicate(){
         let (d,s,p)=setup();
         assert!(s.append_v2_revision_journal_review(JP::AfterFileSync).is_err());
         let r=s.verify_v2_revision_journal_review(None).unwrap();
         assert_eq!(r.current_revision,1);
         assert!(s.append_v2_revision_journal_review(JP::None).is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn after_dir_fsync_injected_failure_does_not_return_success_ack(){
         let (d,s,p)=setup();
         assert!(s.append_v2_revision_journal_review(JP::AfterDirSync).is_err());
         assert_eq!(s.verify_v2_revision_journal_review(None).unwrap().current_revision,1);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn journal_symlink_must_not_be_followed_for_reads_or_appends(){
         let (d,s,p)=setup();
         let other=d.join("other");fs::write(&other,b"secret").unwrap();
         #[cfg(unix)]{
             std::os::unix::fs::symlink(&other,journal_path(&p)).unwrap();
             assert!(s.verify_v2_revision_journal_review(None).is_err());
             assert!(s.append_v2_revision_journal_review(JP::None).is_err());
             assert_eq!(fs::read(&other).unwrap(),b"secret");
         }
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn competing_journal_append_one_wins_other_denied_under_owner_flock(){
         let (d,s,p)=setup();
         let bar=Arc::new(Barrier::new(7));let mut handles=Vec::new();
         for _ in 0..6{
             let p=p.clone();let b=bar.clone();
             handles.push(thread::spawn(move||{
                 let st=NativeStateStore::open(&p).unwrap();
                 b.wait();st.append_v2_revision_journal_review(JP::None).is_ok()
             }));
         }
         bar.wait();
         assert_eq!(handles.into_iter().map(|h|h.join().unwrap()).filter(|v|*v).count(),1);
         assert_eq!(s.verify_v2_revision_journal_review(None).unwrap().journal_entries,1);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn memory_anchor_contract_cannot_create_external_authority_or_hedged(){
         let (d,s,p,_,_)=setup_journal_rev4();
         let entries=read_local_journal(&p).unwrap();
         let floor=Floor{original_owner_generation:99,minimum_revision:4,
             exact_entry_sha256:entries.last().unwrap().entry_sha256.clone()};
         let local=review_owner_against_local_journal(&current(&s),&entries,Some(&floor)).unwrap();
         assert!(local.supplied_floor_matches);
         assert!(!local.external_independence_authenticated);
         assert!(!local.may_send_retry_cancel_verify_closed);
         assert_eq!(require_native_mutation_egress(),Err(NativeHardSafetyError::MutationCapabilityNotCertified));
         fs::remove_dir_all(d).unwrap();
     }
 }

 // Astra signed witness protocol. Signing private keys are generated IN TEST
 // MEMORY only; no real remote witness or trusted pin deployment exists.
 mod v2_signed_external_floor_contract_tests {
     use super::*;
     use super::v2_revision_journal_floor_tests::setup_journal_rev4;
     use openssl::{pkey::{PKey,Private},sign::Signer};
     use spotarb_native_market_core::{
         native_trade2f3_v2_signed_floor::{
             signed_floor_message,verify_signed_floor_cryptography,
             review_signed_floor_against_owner_journal,
             classify_owner_journal_crash_window,
             V2SignedFloorPayload as Payload,V2SignedFloorEnvelope as Signed,
             V2ExternallyProvisionedVerifier as Pin,
             V2WitnessError as Error,V2WitnessQuarantine as Q,
             FLOOR_DOMAIN,FLOOR_PROTOCOL_VERSION,
         },
         native_trade2f3_v2_revision_journal::{journal_path,read_local_journal,V2JournalEntry},
         native_trade2f3_v2_versioned_store::V2ReviewState,
         authoritative_state::{NativeStateStore,NativeVersionedReview},
     };
     use std::{fs,path::Path};
     fn hex(bytes:&[u8])->String{
         bytes.iter().map(|b|format!("{b:02x}")).collect()
     }
     fn sign(key:&PKey<Private>,payload:Payload)->Signed{
         let b=signed_floor_message(&payload).unwrap();
         let mut s=Signer::new_without_digest(key).unwrap();
         Signed{payload,ed25519_signature_hex:hex(&s.sign_oneshot_to_vec(&b).unwrap())}
     }
     fn test_key()->PKey<Private>{
         PKey::generate_ed25519().unwrap()
     }
     fn pin(key:&PKey<Private>)->Pin{
         Pin{
             witness_id:"mock-offhost-witness-A".into(),
             ed25519_public_key_hex:hex(&key.raw_public_key().unwrap()),
             expected_nonce_hex:"12".repeat(32),
         }
     }
     fn payload(entries:&[V2JournalEntry],last:usize)->Payload{
         let e=&entries[last];
         Payload{
             schema_version:FLOOR_PROTOCOL_VERSION,
             domain:FLOOR_DOMAIN.to_owned(),
             witness_id:"mock-offhost-witness-A".into(),
             request_nonce_hex:"12".repeat(32),
             original_owner_generation:e.original_owner_generation,
             immutable_genesis_sha256:e.immutable_genesis_sha256.clone(),
             latest_committed_revision:e.revision,
             latest_entry_sha256:e.entry_sha256.clone(),
         }
     }
     fn owner(store:&NativeStateStore)->V2ReviewState{
         match store.load_versioned_review().unwrap().unwrap(){
             NativeVersionedReview::FeeAwareV2(v)=>v,_=>panic!("not V2")
         }
     }
     #[test]
     fn valid_ed25519_pinned_nonce_matching_latest_local_passes_only_diagnostics(){
         let (dir,s,path,_,_)=setup_journal_rev4();
         let entries=read_local_journal(&path).unwrap();
         let k=test_key();let config=pin(&k);
         let signed=sign(&k,payload(&entries,3));
         verify_signed_floor_cryptography(&signed,&config).unwrap();
         let preview=review_signed_floor_against_owner_journal(
             &owner(&s),&entries,&signed,&config).unwrap();
         assert_eq!(preview.owner_revision,4);
         assert_eq!(preview.signed_floor_revision,4);
         assert!(preview.signature_valid);
         assert!(preview.matches_local_tip);
         assert!(!preview.independent_monotonic_witness_proven);
         assert!(!preview.remote_currentness_proven);
         assert!(!preview.trading_authorized);
         let from_store=s.verify_v2_signed_floor_review(&signed,&config).unwrap();
         assert_eq!(preview,from_store);
         assert_eq!(require_native_mutation_egress(),
             Err(NativeHardSafetyError::MutationCapabilityNotCertified));
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn wrong_key_tampered_signature_and_payload_all_fail_closed(){
         let (dir,s,path,_,_)=setup_journal_rev4();
         let entries=read_local_journal(&path).unwrap();
         let k=test_key();let config=pin(&k);
         let signed=sign(&k,payload(&entries,3));
         let mut bad_pin=config.clone();
         bad_pin.ed25519_public_key_hex=hex(&test_key().raw_public_key().unwrap());
         assert_eq!(verify_signed_floor_cryptography(&signed,&bad_pin),
             Err(Error::InvalidSignature));
         let mut tampered=signed.clone();
         tampered.ed25519_signature_hex="00".repeat(64);
         assert_eq!(verify_signed_floor_cryptography(&tampered,&config),
             Err(Error::InvalidSignature));
         let mut tampered=signed.clone();
         tampered.payload.latest_committed_revision=2;
         assert_eq!(verify_signed_floor_cryptography(&tampered,&config),
             Err(Error::InvalidSignature));
         assert!(s.verify_v2_signed_floor_review(&tampered,&config).is_err());
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn wrong_nonce_witness_id_domain_and_unknown_key_refused(){
         let (dir,_s,path,_,_)=setup_journal_rev4();
         let entries=read_local_journal(&path).unwrap();
         let key=test_key();let good=pin(&key);
         let signed=sign(&key,payload(&entries,3));
         let mut config=good.clone();config.expected_nonce_hex="f0".repeat(32);
         assert_eq!(verify_signed_floor_cryptography(&signed,&config),
             Err(Error::IdentityOrChallengeMismatch));
         let mut config=good.clone();config.witness_id="mock-offhost-witness-B".into();
         assert_eq!(verify_signed_floor_cryptography(&signed,&config),
             Err(Error::IdentityOrChallengeMismatch));
         let mut config=good.clone();config.ed25519_public_key_hex.clear();
         assert_eq!(verify_signed_floor_cryptography(&signed,&config),
             Err(Error::NoTrustedKeyProvisioning));
         let mut payload=payload(&entries,3);payload.domain="WRONG-DOMAIN".into();
         let altered=Signed{payload,ed25519_signature_hex:signed.ed25519_signature_hex.clone()};
         assert!(verify_signed_floor_cryptography(&altered,&good).is_err());
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn stale_genuine_signed_floor_is_only_unacknowledged_not_a_success(){
         let (dir,s,path,_,_)=setup_journal_rev4();
         let entries=read_local_journal(&path).unwrap();
         let key=test_key();let cfg=pin(&key);
         let stale=sign(&key,payload(&entries,2));
         assert_eq!(review_signed_floor_against_owner_journal(
             &owner(&s),&entries,&stale,&cfg),
             Err(Error::Quarantined(Q::AwaitingSignedFloorAdvance)));
         assert!(s.verify_v2_signed_floor_review(&stale,&cfg).is_err());
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn signed_floor_ahead_of_local_owner_is_rollback_alarm(){
         let (dir,s,path,_,_)=setup_journal_rev4();
         let entries=read_local_journal(&path).unwrap();
         let key=test_key();let cfg=pin(&key);
         let mut newer=payload(&entries,3);
         newer.latest_committed_revision=5;
         newer.latest_entry_sha256="ad".repeat(32);
         let signed=sign(&key,newer);
         assert_eq!(review_signed_floor_against_owner_journal(
             &owner(&s),&entries,&signed,&cfg),
             Err(Error::Quarantined(Q::LocalBehindSignedFloor)));
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn same_revision_with_different_signed_tip_is_a_fork(){
         let (dir,s,path,_,_)=setup_journal_rev4();
         let entries=read_local_journal(&path).unwrap();
         let key=test_key();let cfg=pin(&key);
         let mut fork=payload(&entries,3);
         fork.latest_entry_sha256="ee".repeat(32);
         let signed=sign(&key,fork);
         assert_eq!(review_signed_floor_against_owner_journal(
             &owner(&s),&entries,&signed,&cfg),
             Err(Error::Quarantined(Q::EqualRevisionDifferentHash)));
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn coherent_owner_plus_journal_rollback_detected_only_with_retained_newer_signed_tip(){
         let (dir,s,path,old_owners,old_logs)=setup_journal_rev4();
         let entries=read_local_journal(&path).unwrap();
         let key=test_key();let cfg=pin(&key);
         let signed_at_4=sign(&key,payload(&entries,3));
         fs::write(&path,&old_owners[2]).unwrap();
         fs::write(journal_path(&path),&old_logs[2]).unwrap();
         assert!(s.verify_v2_revision_journal_review(None).is_ok());
         assert_eq!(s.verify_v2_signed_floor_review(&signed_at_4,&cfg),
             Err(spotarb_native_market_core::authoritative_state::NativeStateError::Invalid(
                 "V2 signed floor mismatch or untrusted")));
         assert_eq!(review_signed_floor_against_owner_journal(
             &owner(&s),&read_local_journal(&path).unwrap(),&signed_at_4,&cfg),
             Err(Error::Quarantined(Q::LocalBehindSignedFloor)));
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn signed_payload_for_foreign_genesis_or_generation_is_not_our_owner(){
         let (dir,s,path,_,_)=setup_journal_rev4();
         let entries=read_local_journal(&path).unwrap();
         let key=test_key();let cfg=pin(&key);
         for mode in 0..2{
             let mut foreign=payload(&entries,3);
             if mode==0{foreign.original_owner_generation+=1;}
             else{foreign.immutable_genesis_sha256="a0".repeat(32);}
             let signed=sign(&key,foreign);
             assert_eq!(review_signed_floor_against_owner_journal(
                 &owner(&s),&entries,&signed,&cfg),
                 Err(Error::IdentityOrChallengeMismatch));
         }
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn test_signing_key_in_same_failure_domain_cannot_assert_remote_independence(){
         let (dir,s,path,_,_)=setup_journal_rev4();
         let entries=read_local_journal(&path).unwrap();
         // If attacker can replace both pin and signature, math alone cannot
         // establish an externally managed witness despite valid Ed25519.
         let attacker=test_key();
         let replacement_pin=pin(&attacker);
         let replacement_receipt=sign(&attacker,payload(&entries,3));
         let r=review_signed_floor_against_owner_journal(
             &owner(&s),&entries,&replacement_receipt,&replacement_pin).unwrap();
         assert!(r.signature_valid);
         assert!(!r.independent_monotonic_witness_proven);
         assert!(!r.remote_currentness_proven);
         assert!(!r.trading_authorized);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn crash_boundary_owner_ahead_journal_ahead_and_equal_are_non_mutating(){
         assert_eq!(classify_owner_journal_crash_window(4,3),
             Some(Q::LocalOwnerJournalDivergence));
         assert_eq!(classify_owner_journal_crash_window(3,4),
             Some(Q::LocalOwnerJournalDivergence));
         assert_eq!(classify_owner_journal_crash_window(4,4),None);
         let (dir,s,path,owners,logs)=setup_journal_rev4();
         let key=test_key();
         let signed=sign(&key,payload(&read_local_journal(&path).unwrap(),3));
         let config=pin(&key);
         // Test first owner ahead after commit but before local journal append.
         fs::write(journal_path(&path),&logs[2]).unwrap();
         assert!(s.verify_v2_signed_floor_review(&signed,&config).is_err());
         assert_eq!(review_signed_floor_against_owner_journal(
             &owner(&s),&read_local_journal(&path).unwrap(),&signed,&config),
             Err(Error::Quarantined(Q::LocalOwnerJournalDivergence)));
         // Then old owner with still new journal.
         fs::write(&path,&owners[2]).unwrap();
         fs::write(journal_path(&path),&logs[3]).unwrap();
         assert!(s.verify_v2_revision_journal_review(None).is_err());
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn signed_floor_verifier_rejects_torn_local_journal_and_untrusted_flags(){
         let (dir,s,path,_,_)=setup_journal_rev4();
         let entries=read_local_journal(&path).unwrap();
         let key=test_key();let cfg=pin(&key);
         let receipt=sign(&key,payload(&entries,3));
         let mut bytes=fs::read(journal_path(&path)).unwrap();bytes.pop();
         fs::write(journal_path(&path),bytes).unwrap();
         assert!(s.verify_v2_signed_floor_review(&receipt,&cfg).is_err());
         assert!(!owner(&s).production_authorized);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn malformed_signatures_public_keys_nonce_are_denied_without_panics(){
         let (dir,_s,path,_,_)=setup_journal_rev4();
         let entries=read_local_journal(&path).unwrap();
         let key=test_key();let cfg=pin(&key);
         let valid=sign(&key,payload(&entries,3));
         for case in 0..5{
             let mut r=valid.clone();
             let mut pin=cfg.clone();
             match case{
                 0=>r.ed25519_signature_hex="a".into(),
                 1=>r.ed25519_signature_hex="FF".repeat(64),
                 2=>pin.ed25519_public_key_hex="a".into(),
                 3=>r.payload.request_nonce_hex="f".into(),
                 _=>r.payload.witness_id="🤖".into(),
             }
             assert!(verify_signed_floor_cryptography(&r,&pin).is_err(),"case {case}");
         }
         fs::remove_dir_all(dir).unwrap();
     }
 }

 // Astra off-host witness interface SIMULATOR only; files and mock private
 // key are on this test VM. Cryptographic success != real external storage.
 mod v2_witness_mock_service_cas_tests {
     use super::*;
     use super::v2_revision_journal_floor_tests::setup_journal_rev4;
     use openssl::pkey::PKey;
     use spotarb_native_market_core::{
         native_trade2f3_v2_revision_journal::{journal_path,read_local_journal,V2JournalEntry},
         native_trade2f3_v2_witness_mock_store::{
             V2OfflineWitnessMockStore as Store,V2MockCommitOutcome as ResultKind,
             V2MockWitnessFp as Fp,
         },
         native_trade2f3_v2_signed_floor::{
             V2ExternallyProvisionedVerifier as Pin,
             verify_signed_floor_cryptography,
             review_signed_floor_against_owner_journal,
             V2WitnessError as Error,V2WitnessQuarantine as Q,
         },
         authoritative_state::{NativeStateStore,NativeVersionedReview},
     };
     use std::{fs,path::{Path,PathBuf},sync::{Arc,Barrier},thread};
     fn hex(bytes:&[u8])->String{
         bytes.iter().map(|b|format!("{b:02x}")).collect()
     }
     pub(super) fn fixture()->(PathBuf,NativeStateStore,PathBuf,Vec<Vec<u8>>,Vec<Vec<u8>>,PathBuf,Store,Vec<V2JournalEntry>){
         let (dir,s,path,owners,logs)=setup_journal_rev4();
         let other=dir.join("MOCK_WITNESS_DIFFERENT_DIRECTORY_SAME_HOST");
         fs::create_dir(&other).unwrap();
         let server=Store::open(&other).unwrap();
         let entries=read_local_journal(&path).unwrap();
         (dir,s,path,owners,logs,other,server,entries)
     }
     pub(super) fn key()->PKey<openssl::pkey::Private>{PKey::generate_ed25519().unwrap()}
     pub(super) fn pin(k:&PKey<openssl::pkey::Private>,nonce:&str)->Pin{
         Pin{
             witness_id:"isolated-mock-witness".into(),
             ed25519_public_key_hex:hex(&k.raw_public_key().unwrap()),
             expected_nonce_hex:nonce.to_owned(),
         }
     }
     pub(super) fn owner(s:&NativeStateStore)->spotarb_native_market_core::native_trade2f3_v2_versioned_store::V2ReviewState{
         match s.load_versioned_review().unwrap().unwrap(){
             NativeVersionedReview::FeeAwareV2(v)=>v,_=>panic!("V2 expected")
         }
     }
     pub(super) fn commit_all(store:&Store,entries:&[V2JournalEntry]){
         for e in entries{
             assert_eq!(store.commit_offline(e,Fp::None).unwrap(),ResultKind::Committed);
         }
     }
     #[test]
     fn persistent_cold_reopen_exact_cas_and_nonce_signed_tip_no_trade_authority(){
         let (dir,s,path,_,_,root,store,entries)=fixture();
         commit_all(&store,&entries);
         let reloaded=Store::open(&root).unwrap();
         assert_eq!(reloaded.latest_offline().unwrap(),Some(entries[3].clone()));
         let k=key();let nonce="ac".repeat(32);
         let signed=reloaded.latest_signed_offline("isolated-mock-witness",&nonce,&k).unwrap();
         let config=pin(&k,&nonce);
         verify_signed_floor_cryptography(&signed,&config).unwrap();
         let review=review_signed_floor_against_owner_journal(
             &owner(&s),&read_local_journal(&path).unwrap(),&signed,&config).unwrap();
         assert_eq!(review.owner_revision,4);
         assert_eq!(review.signed_floor_revision,4);
         assert!(review.signature_valid);
         assert!(!review.independent_monotonic_witness_proven);
         assert!(!review.remote_currentness_proven);
         assert!(!review.trading_authorized);
         assert_eq!(require_native_mutation_egress(),
             Err(NativeHardSafetyError::MutationCapabilityNotCertified));
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn fresh_challenge_needed_on_read_replay_old_nonce_denied(){
         let (dir,_s,_p,_,_,_,store,entries)=fixture();
         commit_all(&store,&entries);
         let k=key();let old="01".repeat(32);let new="02".repeat(32);
         let original=store.latest_signed_offline("isolated-mock-witness",&old,&k).unwrap();
         assert!(verify_signed_floor_cryptography(&original,&pin(&k,&new)).is_err());
         let fresh=store.latest_signed_offline("isolated-mock-witness",&new,&k).unwrap();
         assert!(verify_signed_floor_cryptography(&fresh,&pin(&k,&new)).is_ok());
         assert_ne!(original.ed25519_signature_hex,fresh.ed25519_signature_hex);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn replay_exact_tip_after_lost_ack_is_idempotent_but_no_new_write(){
         let (dir,_,_,_,_,_,store,entries)=fixture();
         commit_all(&store,&entries[..3]);
         assert!(store.commit_offline(&entries[3],Fp::AfterRename).is_err());
         let seen=store.latest_offline().unwrap().unwrap();
         assert_eq!(seen,entries[3]);
         assert_eq!(store.commit_offline(&entries[3],Fp::None).unwrap(),
             ResultKind::AlreadyCommitted);
         assert_eq!(store.latest_offline().unwrap().unwrap().revision,4);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn wrong_parent_foreign_owner_fork_out_of_order_and_missing_genesis_rejected(){
         for mode in 0..6{
             let (dir,_,_,_,_,_,store,entries)=fixture();
             if mode!=0{commit_all(&store,&entries[..2]);}
             let mut candidate=entries[2].clone();
             match mode{
                 0=>{},
                 1=>candidate.revision+=1,
                 2=>candidate.original_owner_generation+=1,
                 3=>candidate.immutable_genesis_sha256="c0".repeat(32),
                 4=>candidate.previous_entry_sha256="ff".repeat(32),
                 _=>candidate.entry_sha256="a1".repeat(32),
             }
             assert!(store.commit_offline(&candidate,Fp::None).is_err(),"case {mode}");
             let last=store.latest_offline().unwrap();
             assert_eq!(last.map(|v|v.revision),if mode==0{None}else{Some(2)});
             fs::remove_dir_all(dir).unwrap();
         }
     }
     #[test]
     fn current_tip_conflicting_hash_cannot_override_committed_revision(){
         let (dir,_,_,_,_,_,store,entries)=fixture();
         commit_all(&store,&entries);
         // An earlier frame is not an idempotent retry of latest.
         assert!(store.commit_offline(&entries[2],Fp::None).is_err());
         assert_eq!(store.latest_offline().unwrap(),Some(entries[3].clone()));
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn failpoint_before_write_keeps_old_committed_tip(){
         let (dir,_,_,_,_,_,store,entries)=fixture();
         commit_all(&store,&entries[..3]);
         assert!(store.commit_offline(&entries[3],Fp::BeforeWrite).is_err());
         assert_eq!(store.latest_offline().unwrap().unwrap().revision,3);
         assert_eq!(store.commit_offline(&entries[3],Fp::None).unwrap(),
             ResultKind::Committed);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn partial_pending_and_after_filesync_are_quarantined_not_autorepaired(){
         for fp in [Fp::PartialStage,Fp::AfterFileSync]{
             let (dir,_,_,_,_,_,store,entries)=fixture();
             commit_all(&store,&entries[..3]);
             assert!(store.commit_offline(&entries[3],fp).is_err());
             assert!(store.latest_offline().is_err());
             assert!(store.commit_offline(&entries[3],Fp::None).is_err());
             fs::remove_dir_all(dir).unwrap();
         }
     }
     #[test]
     fn after_dirsync_ambiguous_return_requires_fresh_read_and_exact_idempotence(){
         let (dir,_,_,_,_,root,store,entries)=fixture();
         commit_all(&store,&entries[..3]);
         assert!(store.commit_offline(&entries[3],Fp::AfterDirSync).is_err());
         let cold=Store::open(root).unwrap();
         assert_eq!(cold.latest_offline().unwrap(),Some(entries[3].clone()));
         assert_eq!(cold.commit_offline(&entries[3],Fp::None).unwrap(),
             ResultKind::AlreadyCommitted);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn local_both_files_rollback_and_mock_witness_preserved_blocks_signed_floor(){
         let (dir,s,path,old_owners,old_logs,_,store,entries)=fixture();
         commit_all(&store,&entries);
         let k=key();let nonce="44".repeat(32);
         let witness_read=store.latest_signed_offline("isolated-mock-witness",&nonce,&k).unwrap();
         fs::write(&path,&old_owners[2]).unwrap();
         fs::write(journal_path(&path),&old_logs[2]).unwrap();
         assert!(s.verify_v2_revision_journal_review(None).is_ok()); // both rolled back
         assert_eq!(review_signed_floor_against_owner_journal(
             &owner(&s),&read_local_journal(&path).unwrap(),&witness_read,&pin(&k,&nonce)),
             Err(Error::Quarantined(Q::LocalBehindSignedFloor)));
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn rollback_all_three_files_still_escapes_same_host_mock_no_false_claim(){
         let (dir,s,path,old_owners,old_logs,root,store,entries)=fixture();
         commit_all(&store,&entries[..3]);
         let witness_at3=fs::read(root.join("witness_state.json")).unwrap();
         assert_eq!(store.commit_offline(&entries[3],Fp::None).unwrap(),ResultKind::Committed);
         // An attacker with host write privileges rolls back *all* copies.
         fs::write(&path,&old_owners[2]).unwrap();
         fs::write(journal_path(&path),&old_logs[2]).unwrap();
         fs::write(root.join("witness_state.json"),witness_at3).unwrap();
         let k=key();let nonce="56".repeat(32);
         let latest_mock=store.latest_signed_offline("isolated-mock-witness",&nonce,&k).unwrap();
         let review=review_signed_floor_against_owner_journal(
             &owner(&s),&read_local_journal(&path).unwrap(),&latest_mock,&pin(&k,&nonce)).unwrap();
         assert!(review.signature_valid&&review.matches_local_tip);
         assert!(!review.independent_monotonic_witness_proven);
         assert!(!review.trading_authorized);
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn six_independent_cas_contenders_commit_next_frame_exactly_once(){
         let (dir,_,_,_,_,root,store,entries)=fixture();
         commit_all(&store,&entries[..3]);
         let barrier=Arc::new(Barrier::new(7));let mut workers=Vec::new();
         for _ in 0..6{
             let root=root.clone();let entry=entries[3].clone();let b=barrier.clone();
             workers.push(thread::spawn(move||{
                 let store=Store::open(root).unwrap();b.wait();
                 store.commit_offline(&entry,Fp::None).unwrap()
             }));
         }
         barrier.wait();
         let outcomes=workers.into_iter().map(|w|w.join().unwrap()).collect::<Vec<_>>();
         assert_eq!(outcomes.iter().filter(|o|**o==ResultKind::Committed).count(),1);
         assert_eq!(outcomes.iter().filter(|o|**o==ResultKind::AlreadyCommitted).count(),5);
         assert_eq!(store.latest_offline().unwrap(),Some(entries[3].clone()));
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn real_file_corruption_symlink_state_and_invalid_nonce_fail_closed(){
         for mode in 0..4 {
             let (dir,_,_,_,_,root,store,entries)=fixture();
             commit_all(&store,&entries[..3]);
             match mode{
                 0=>{
                     fs::write(root.join("witness_state.json"),b"{}").unwrap();
                     assert!(store.latest_offline().is_err());
                 }
                 1=>{
                     let bytes=fs::read(root.join("witness_state.json")).unwrap();
                     fs::write(root.join("witness_state.json"),&bytes[..bytes.len()/2]).unwrap();
                     assert!(store.latest_offline().is_err());
                 }
                 2=>{
                     let state=root.join("witness_state.json");
                     fs::remove_file(&state).unwrap();
                     #[cfg(unix)]{
                         std::os::unix::fs::symlink("/dev/null",state).unwrap();
                     }
                     assert!(store.latest_offline().is_err());
                 }
                 _=>{
                     let k=key();
                     assert!(store.latest_signed_offline("isolated-mock-witness","bad",&k).is_err());
                 }
             }
             fs::remove_dir_all(dir).unwrap();
         }
     }
     #[test]
     fn simulated_crash_boundary_owner_ahead_of_local_journal_requires_quarantine(){
         let (dir,s,path,_,logs,_,store,entries)=fixture();
         commit_all(&store,&entries);
         fs::write(journal_path(&path),&logs[2]).unwrap();
         let k=key();let nonce="ab".repeat(32);
         let signed=store.latest_signed_offline("isolated-mock-witness",&nonce,&k).unwrap();
         assert!(s.verify_v2_signed_floor_review(&signed,&pin(&k,&nonce)).is_err());
         assert!(s.verify_v2_revision_journal_review(None).is_err());
         fs::remove_dir_all(dir).unwrap();
     }
     #[test]
     fn journal_ahead_of_witness_must_not_ack_latest_external_floor(){
         let (dir,s,path,_,_,_,store,entries)=fixture();
         commit_all(&store,&entries[..3]);
         let key=key();let nonce="ce".repeat(32);
         let stale=store.latest_signed_offline("isolated-mock-witness",&nonce,&key).unwrap();
         assert_eq!(review_signed_floor_against_owner_journal(
             &owner(&s),&read_local_journal(&path).unwrap(),&stale,&pin(&key,&nonce)),
             Err(Error::Quarantined(Q::AwaitingSignedFloorAdvance)));
         fs::remove_dir_all(dir).unwrap();
     }
 }

mod v2_three_way_crash_diagnostic_tests {
    use super::*;
    use super::v2_witness_mock_service_cas_tests::{fixture,key,pin,owner,commit_all};
    use spotarb_native_market_core::{
        native_trade2f3_v2_revision_journal::{journal_path,read_local_journal},
        native_trade2f3_v2_signed_floor::V2SignedFloorEnvelope as Signed,
        native_trade2f3_v2_witness_mock_store::{
            V2OfflineWitnessMockStore as Store,V2MockWitnessFp as Fp,
        },
        native_trade2f3_v2_three_way_gate::{
            classify_v2_three_way as classify,
            V2ThreeWayCondition as Cond,V2ThreeWayError as Error,
            V2ThreeWayReview,
        },
        authoritative_state::NativeStateStore,
    };
    use std::fs;
    fn receipt(s:&Store,k:&openssl::pkey::PKey<openssl::pkey::Private>,nonce:&str)->Signed{
        s.latest_signed_offline("isolated-mock-witness",nonce,k).unwrap()
    }
    fn blocked(r:&V2ThreeWayReview,expected:Cond){
        assert_eq!(r.condition,expected);
        assert!(r.signed_witness_valid);
        assert!(!r.witness_is_independent_and_latest);
        assert!(!r.may_repair_replay_or_retry_automatically);
        assert!(!r.may_issue_exchange_commands);
        assert!(!r.may_ack_durable_external_commit);
    }
    #[test]
    fn matched_three_way_is_only_diagnostic_even_after_cold_open(){
        let (d,_,p,_,_,_,w,entries)=fixture();
        commit_all(&w,&entries);
        let k=key();let nonce="31".repeat(32);
        let cold=NativeStateStore::open(&p).unwrap();
        let r=cold.classify_v2_three_way_review(&receipt(&w,&k,&nonce),&pin(&k,&nonce)).unwrap();
        blocked(&r,Cond::AlignedDiagnosticOnly);
        assert_eq!((r.owner_revision,r.journal_revision,r.signed_witness_revision),(4,4,4));
        assert_eq!(require_native_mutation_egress(),
            Err(NativeHardSafetyError::MutationCapabilityNotCertified));
        fs::remove_dir_all(d).unwrap();
    }
    #[test]
    fn owner_ahead_before_journal_append_is_quarantined(){
        let (d,s,p,_,logs,_,w,entries)=fixture();
        commit_all(&w,&entries[..3]);
        fs::write(journal_path(&p),&logs[2]).unwrap();
        let k=key();let nonce="32".repeat(32);
        let r=s.classify_v2_three_way_review(&receipt(&w,&k,&nonce),&pin(&k,&nonce)).unwrap();
        blocked(&r,Cond::OwnerAheadOfJournal);
        fs::remove_dir_all(d).unwrap();
    }
    #[test]
    fn owner_behind_committed_journal_and_witness_quarantined(){
        let (d,s,p,owners,_,_,w,entries)=fixture();
        commit_all(&w,&entries);
        fs::write(&p,&owners[2]).unwrap();
        let k=key();let nonce="33".repeat(32);
        let r=s.classify_v2_three_way_review(&receipt(&w,&k,&nonce),&pin(&k,&nonce)).unwrap();
        blocked(&r,Cond::OwnerBehindJournal);
        fs::remove_dir_all(d).unwrap();
    }
    #[test]
    fn journal_ahead_of_signed_witness_requires_pending_ack_quarantine(){
        let (d,s,_,_,_,_,w,entries)=fixture();
        commit_all(&w,&entries[..3]);
        let k=key();let nonce="34".repeat(32);
        let r=s.classify_v2_three_way_review(&receipt(&w,&k,&nonce),&pin(&k,&nonce)).unwrap();
        blocked(&r,Cond::LocalAheadOfWitness);
        fs::remove_dir_all(d).unwrap();
    }
    #[test]
    fn signed_witness_ahead_of_coherently_rolled_back_local_pair(){
        let (d,s,p,owners,logs,_,w,entries)=fixture();
        commit_all(&w,&entries);
        fs::write(&p,&owners[2]).unwrap();
        fs::write(journal_path(&p),&logs[2]).unwrap();
        let k=key();let nonce="35".repeat(32);
        let r=s.classify_v2_three_way_review(&receipt(&w,&k,&nonce),&pin(&k,&nonce)).unwrap();
        blocked(&r,Cond::WitnessAheadOfLocal);
        fs::remove_dir_all(d).unwrap();
    }
    #[test]
    fn signed_tampering_and_wrong_request_nonce_denied_before_classification(){
        let (d,s,_,_,_,_,w,entries)=fixture();
        commit_all(&w,&entries);
        let k=key();let nonce="36".repeat(32);
        let original=receipt(&w,&k,&nonce);
        let mut altered=original.clone();
        altered.ed25519_signature_hex="00".repeat(64);
        assert_eq!(classify(&owner(&s),&entries,&altered,&pin(&k,&nonce)),
            Err(Error::BadWitnessSignatureOrChallenge));
        assert_eq!(classify(&owner(&s),&entries,&original,&pin(&k,&"37".repeat(32))),
            Err(Error::BadWitnessSignatureOrChallenge));
        fs::remove_dir_all(d).unwrap();
    }
    #[test]
    fn invalid_local_hash_chain_and_missing_middle_frame_fail_closed(){
        let (d,s,p,_,_,_,w,entries)=fixture();
        commit_all(&w,&entries);
        let k=key();let nonce="38".repeat(32);
        let original=receipt(&w,&k,&nonce);
        let mut damaged=entries.clone();
        damaged[3].entry_sha256="00".repeat(32);
        assert_eq!(classify(&owner(&s),&damaged,&original,&pin(&k,&nonce)),
            Err(Error::InvalidLocalStateOrJournal));
        let missing=vec![entries[0].clone(),entries[1].clone(),entries[3].clone()];
        assert_eq!(classify(&owner(&s),&missing,&original,&pin(&k,&nonce)),
            Err(Error::InvalidLocalStateOrJournal));
        let mut bytes=fs::read(journal_path(&p)).unwrap();bytes.pop();
        fs::write(journal_path(&p),bytes).unwrap();
        assert!(s.classify_v2_three_way_review(&original,&pin(&k,&nonce)).is_err());
        fs::remove_dir_all(d).unwrap();
    }
    #[test]
    fn witness_after_rename_lost_ack_requires_fresh_read_and_denies_durable_ack(){
        let (d,s,_,_,_,_,w,entries)=fixture();
        commit_all(&w,&entries[..3]);
        assert!(w.commit_offline(&entries[3],Fp::AfterRename).is_err());
        let k=key();let nonce="39".repeat(32);
        let r=s.classify_v2_three_way_review(&receipt(&w,&k,&nonce),&pin(&k,&nonce)).unwrap();
        blocked(&r,Cond::AlignedDiagnosticOnly);
        fs::remove_dir_all(d).unwrap();
    }
    #[test]
    fn complete_same_host_three_file_rollback_still_looks_aligned_but_never_trusted(){
        let (d,s,p,owners,logs,root,w,entries)=fixture();
        commit_all(&w,&entries[..3]);
        let old=fs::read(root.join("witness_state.json")).unwrap();
        w.commit_offline(&entries[3],Fp::None).unwrap();
        fs::write(&p,&owners[2]).unwrap();
        fs::write(journal_path(&p),&logs[2]).unwrap();
        fs::write(root.join("witness_state.json"),old).unwrap();
        let k=key();let nonce="40".repeat(32);
        let r=s.classify_v2_three_way_review(&receipt(&w,&k,&nonce),&pin(&k,&nonce)).unwrap();
        blocked(&r,Cond::AlignedDiagnosticOnly);
        assert_eq!(r.owner_revision,3);
        fs::remove_dir_all(d).unwrap();
    }
}

 // Rust P0-03 offline durable ACK protocol: all state is same-host fixture.
 // No production mutator or independent remote signer is reachable.
 mod v2_durable_ack_prepared_signed_tests {
     use super::*;
     use super::v2_witness_mock_service_cas_tests::{fixture,key,pin,commit_all};
     use spotarb_native_market_core::{
         authoritative_state::NativeStateStore,
         native_trade2f3_v2_durable_ack_review::{
             V2DurableAckReview as Ack,V2AckReviewFailpoint as Fp,
             V2AckReviewOutcome as Outcome,V2AckReviewPhase as Phase,
         },
         native_trade2f3_v2_witness_mock_store::V2MockWitnessFp as Wfp,
         native_trade2f3_v2_revision_journal::journal_path,
     };
     use std::{fs,sync::{Arc,Barrier},thread};
     fn setup()->(
         std::path::PathBuf,NativeStateStore,std::path::PathBuf,
         Vec<Vec<u8>>,Vec<Vec<u8>>,
         spotarb_native_market_core::native_trade2f3_v2_witness_mock_store::V2OfflineWitnessMockStore,
         Vec<spotarb_native_market_core::native_trade2f3_v2_revision_journal::V2JournalEntry>,
         std::path::PathBuf,Ack,
     ){
         let (d,s,p,owners,logs,_wroot,w,entries)=fixture();
         let ar=d.join("ACK_REVIEW_SAME_HOST");
         fs::create_dir(&ar).unwrap();
         let ack=Ack::open(&ar).unwrap();
         (d,s,p,owners,logs,w,entries,ar,ack)
     }
     #[test]
     fn stage_then_signed_latest_ends_local_only_and_survives_cold_reopen(){
         let (d,s,p,_,_,w,entries,root,ack)=setup();
         commit_all(&w,&entries[..3]);
         assert_eq!(s.prepare_v2_ack_intent_review(&ack,Some(&entries[2]),Fp::None).unwrap(),
             Outcome::Created);
         assert_eq!(ack.read_review().unwrap().unwrap().phase,Phase::Prepared);
         assert_eq!(w.commit_offline(&entries[3],Wfp::None).unwrap(),
             spotarb_native_market_core::native_trade2f3_v2_witness_mock_store::V2MockCommitOutcome::Committed);
         let k=key();let nonce="51".repeat(32);
         let receipt=w.latest_signed_offline("isolated-mock-witness",&nonce,&k).unwrap();
         assert_eq!(s.confirm_v2_ack_intent_review(&ack,&receipt,&pin(&k,&nonce),Fp::None).unwrap(),
             Outcome::LocallyConfirmed);
         let cold_ack=Ack::open(root).unwrap();
         let cold_owner=NativeStateStore::open(p).unwrap();
         assert_eq!(cold_ack.read_review().unwrap().unwrap().phase,Phase::LocallyConfirmed);
         let new_nonce="52".repeat(32);
         let fresh=w.latest_signed_offline("isolated-mock-witness",&new_nonce,&k).unwrap();
         assert_eq!(cold_owner.confirm_v2_ack_intent_review(&cold_ack,&fresh,
             &pin(&k,&new_nonce),Fp::None).unwrap(),Outcome::LocallyConfirmed);
         assert_eq!(require_native_mutation_egress(),
             Err(NativeHardSafetyError::MutationCapabilityNotCertified));
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn missing_prepared_intent_cannot_accept_even_correct_signed_floor(){
         let (d,s,_,_,_,w,entries,_,ack)=setup();
         commit_all(&w,&entries);
         let k=key();let nonce="53".repeat(32);
         let receipt=w.latest_signed_offline("isolated-mock-witness",&nonce,&k).unwrap();
         assert!(s.confirm_v2_ack_intent_review(&ack,&receipt,&pin(&k,&nonce),Fp::None).is_err());
         assert!(ack.read_review().unwrap().is_none());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn witness_predecessor_must_be_exact_even_if_caller_claims_signed(){
         let (d,s,_,_,_,w,entries,_,ack)=setup();
         commit_all(&w,&entries[..2]);
         assert!(s.prepare_v2_ack_intent_review(&ack,w.latest_offline().unwrap().as_ref(),Fp::None).is_err());
         assert!(ack.read_review().unwrap().is_none());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn witness_at_previous_revision_denies_confirmation_and_preserves_prepared(){
         let (d,s,_,_,_,w,entries,_,ack)=setup();
         commit_all(&w,&entries[..3]);
         s.prepare_v2_ack_intent_review(&ack,Some(&entries[2]),Fp::None).unwrap();
         let k=key();let nonce="54".repeat(32);
         let stale=w.latest_signed_offline("isolated-mock-witness",&nonce,&k).unwrap();
         assert!(s.confirm_v2_ack_intent_review(&ack,&stale,&pin(&k,&nonce),Fp::None).is_err());
         assert_eq!(ack.read_review().unwrap().unwrap().phase,Phase::Prepared);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn torn_prepared_pending_file_quarantines_every_reopen(){
         let (d,s,_,_,_,w,entries,root,ack)=setup();
         commit_all(&w,&entries[..3]);
         assert!(s.prepare_v2_ack_intent_review(&ack,Some(&entries[2]),Fp::PartialWrite).is_err());
         let cold=Ack::open(root).unwrap();
         assert!(cold.read_review().is_err());
         assert!(s.prepare_v2_ack_intent_review(&cold,Some(&entries[2]),Fp::None).is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn fsynced_prepared_but_lost_ack_remains_ambiguous_pending(){
         let (d,s,_,_,_,w,entries,root,ack)=setup();
         commit_all(&w,&entries[..3]);
         assert!(s.prepare_v2_ack_intent_review(&ack,Some(&entries[2]),Fp::AfterFileSync).is_err());
         assert!(Ack::open(root).unwrap().read_review().is_err());
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn after_rename_prepared_loss_reopens_exactly_once_without_new_write(){
         let (d,s,_,_,_,w,entries,root,ack)=setup();
         commit_all(&w,&entries[..3]);
         assert!(s.prepare_v2_ack_intent_review(&ack,Some(&entries[2]),Fp::AfterRename).is_err());
         let cold=Ack::open(root).unwrap();
         assert_eq!(cold.read_review().unwrap().unwrap().phase,Phase::Prepared);
         assert_eq!(s.prepare_v2_ack_intent_review(&cold,Some(&entries[2]),Fp::None).unwrap(),
             Outcome::AlreadyPrepared);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn after_rename_local_confirmation_must_reverify_fresh_signed_witness(){
         let (d,s,_,_,_,w,entries,root,ack)=setup();
         commit_all(&w,&entries[..3]);
         s.prepare_v2_ack_intent_review(&ack,Some(&entries[2]),Fp::None).unwrap();
         w.commit_offline(&entries[3],Wfp::None).unwrap();
         let k=key();let nonce="55".repeat(32);
         let signed=w.latest_signed_offline("isolated-mock-witness",&nonce,&k).unwrap();
         assert!(s.confirm_v2_ack_intent_review(&ack,&signed,
             &pin(&k,&nonce),Fp::AfterRename).is_err());
         let cold=Ack::open(root).unwrap();
         assert_eq!(cold.read_review().unwrap().unwrap().phase,Phase::LocallyConfirmed);
         assert!(s.confirm_v2_ack_intent_review(&cold,&signed,
             &pin(&k,&"56".repeat(32)),Fp::None).is_err());
         assert_eq!(cold.read_review().unwrap().unwrap().phase,Phase::LocallyConfirmed);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn owner_or_journal_rollback_after_intent_denies_new_local_ack(){
         for mode in 0..2 {
             let (d,s,p,owners,logs,w,entries,_,ack)=setup();
             commit_all(&w,&entries[..3]);
             s.prepare_v2_ack_intent_review(&ack,Some(&entries[2]),Fp::None).unwrap();
             w.commit_offline(&entries[3],Wfp::None).unwrap();
             if mode==0{fs::write(&p,&owners[2]).unwrap();}
             else{fs::write(journal_path(&p),&logs[2]).unwrap();}
             let k=key();let nonce="57".repeat(32);
             let signed=w.latest_signed_offline("isolated-mock-witness",&nonce,&k).unwrap();
             assert!(s.confirm_v2_ack_intent_review(&ack,&signed,&pin(&k,&nonce),Fp::None).is_err());
             assert_eq!(ack.read_review().unwrap().unwrap().phase,Phase::Prepared);
             fs::remove_dir_all(d).unwrap();
         }
     }
     #[test]
     fn concurrent_prepared_requests_have_one_durable_creator(){
         let (d,_,p,_,_,w,entries,root,ack)=setup();
         commit_all(&w,&entries[..3]);
         let b=Arc::new(Barrier::new(7));
         let mut threads=Vec::new();
         for _ in 0..6{
             let owner_path=p.clone();let ack_root=root.clone();
             let prior=entries[2].clone();let bar=b.clone();
             threads.push(thread::spawn(move||{
                 let st=NativeStateStore::open(owner_path).unwrap();
                 let a=Ack::open(ack_root).unwrap();
                 bar.wait();
                 st.prepare_v2_ack_intent_review(&a,Some(&prior),Fp::None).unwrap()
             }));
         }
         b.wait();
         let outcomes=threads.into_iter().map(|h|h.join().unwrap()).collect::<Vec<_>>();
         assert_eq!(outcomes.iter().filter(|v|**v==Outcome::Created).count(),1);
         assert_eq!(outcomes.iter().filter(|v|**v==Outcome::AlreadyPrepared).count(),5);
         assert_eq!(ack.read_review().unwrap().unwrap().revision,4);
         fs::remove_dir_all(d).unwrap();
     }
 }

 // Offline transport-neutral signed client CAS + signed server CAS receipt.
 // All Ed25519 private keys are generated only in these in-memory fixtures.
 mod v2_signed_remote_cas_transport_contract_tests {
     use super::*;
     use super::v2_witness_mock_service_cas_tests::{fixture,key,commit_all};
     use openssl::{pkey::{PKey,Private},sign::Signer};
     use spotarb_native_market_core::{
         native_trade2f3_v2_remote_cas_contract::{
             V2CasWriteRequest as Request,V2SignedCasWrite as Command,
             V2CasPinnedIdentity as Pins,V2SignedCasReceipt as SignedReceipt,
             V2CasResultKind as ResultKind,V2CasContractError as Error,
             CAS_REQUEST_DOMAIN,cas_request_message,cas_receipt_message,
             offline_receipt_frame,verify_signed_cas_command,
             verify_signed_cas_response,
         },
         native_trade2f3_v2_witness_mock_store::{
             V2MockWitnessFp as Wfp,V2MockCommitOutcome as CommitKind,
         },
         native_trade2f3_v2_revision_journal::V2JournalEntry,
     };
     use std::fs;
     fn hex(b:&[u8])->String{b.iter().map(|v|format!("{v:02x}")).collect()}
     fn signed(k:&PKey<Private>,payload:&[u8])->String{
         let mut signer=Signer::new_without_digest(k).unwrap();
         hex(&signer.sign_oneshot_to_vec(payload).unwrap())
     }
     fn command(entries:&[V2JournalEntry],client:&PKey<Private>,
         nonce:&str,operation:&str)->Command{
         let r=Request{
             version:1,domain:CAS_REQUEST_DOMAIN.into(),
             witness_id:"remote-witness-contract-A".into(),
             client_id:"bot-review-client-A".into(),
             operation_id_hex:operation.to_owned(),
             challenge_hex:nonce.to_owned(),
             candidate:entries[3].clone(),
             expected_predecessor_revision:3,
             expected_predecessor_entry_sha256:Some(entries[2].entry_sha256.clone()),
         };
         let sig=signed(client,&cas_request_message(&r).unwrap());
         Command{request:r,client_ed25519_signature_hex:sig}
     }
     fn pins(client:&PKey<Private>,witness:&PKey<Private>)->Pins{
         Pins{
             witness_id:"remote-witness-contract-A".into(),
             client_id:"bot-review-client-A".into(),
             client_public_key_hex:hex(&client.raw_public_key().unwrap()),
             witness_public_key_hex:hex(&witness.raw_public_key().unwrap()),
         }
     }
     fn receipt(cmd:&Command,witness:&PKey<Private>,result:ResultKind)->SignedReceipt{
         let r=offline_receipt_frame(cmd,result).unwrap();
         let sig=signed(witness,&cas_receipt_message(&r).unwrap());
         SignedReceipt{receipt:r,witness_ed25519_signature_hex:sig}
     }
     #[test]
     fn authenticated_signed_cas_roundtrip_mock_has_no_remote_authority(){
         let (d,_,_,_,_,_,store,entries)=fixture();
         commit_all(&store,&entries[..3]);
         let client=key();let witness=key();let p=pins(&client,&witness);
         let cmd=command(&entries,&client,&"a1".repeat(32),&"b1".repeat(32));
         verify_signed_cas_command(&cmd,&p).unwrap();
         assert_eq!(store.accept_signed_cas_offline(&cmd,&p,Wfp::None).unwrap(),
             CommitKind::Committed);
         let r=receipt(&cmd,&witness,ResultKind::Committed);
         let accepted=verify_signed_cas_response(&cmd,&r,&p).unwrap();
         assert_eq!(accepted.revision,4);
         assert!(accepted.signed_request_valid&&accepted.signed_reply_valid);
         assert!(!accepted.independently_durable_remote_commit_proven);
         assert!(!accepted.remote_currentness_proven);
         assert!(!accepted.trading_authorized);
         assert_eq!(require_native_mutation_egress(),
             Err(NativeHardSafetyError::MutationCapabilityNotCertified));
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn exact_replay_from_mock_witness_idempotent_but_signed_ack_not_authority(){
         let (d,_,_,_,_,_,store,entries)=fixture();
         commit_all(&store,&entries[..3]);
         let client=key();let witness=key();let p=pins(&client,&witness);
         let cmd=command(&entries,&client,&"a2".repeat(32),&"b2".repeat(32));
         assert_eq!(store.accept_signed_cas_offline(&cmd,&p,Wfp::None).unwrap(),
             CommitKind::Committed);
         assert_eq!(store.accept_signed_cas_offline(&cmd,&p,Wfp::None).unwrap(),
             CommitKind::AlreadyCommitted);
         let ack=receipt(&cmd,&witness,ResultKind::AlreadyCommitted);
         let checked=verify_signed_cas_response(&cmd,&ack,&p).unwrap();
         assert_eq!(checked.result,ResultKind::AlreadyCommitted);
         assert!(!checked.independently_durable_remote_commit_proven);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn signed_response_cannot_be_reused_with_new_challenge_or_operation(){
         let (d,_,_,_,_,_,_,entries)=fixture();
         let c=key();let w=key();let p=pins(&c,&w);
         let original=command(&entries,&c,&"a3".repeat(32),&"b3".repeat(32));
         let old=receipt(&original,&w,ResultKind::Committed);
         for mode in 0..2{
             let new=if mode==0{
                 command(&entries,&c,&"a4".repeat(32),&"b3".repeat(32))
             }else{
                 command(&entries,&c,&"a3".repeat(32),&"b4".repeat(32))
             };
             assert_eq!(verify_signed_cas_response(&new,&old,&p),
                 Err(Error::ReplayOrDifferentRequest));
         }
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn wrong_client_key_and_witness_key_forgery_are_detected(){
         let (d,_,_,_,_,_,_,entries)=fixture();
         let c=key();let w=key();let other=key();
         let cmd=command(&entries,&c,&"a5".repeat(32),&"b5".repeat(32));
         let ack=receipt(&cmd,&w,ResultKind::Committed);
         let mut p=pins(&c,&w);
         p.client_public_key_hex=hex(&other.raw_public_key().unwrap());
         assert_eq!(verify_signed_cas_command(&cmd,&p),Err(Error::InvalidSignature));
         p=pins(&c,&other);
         assert_eq!(verify_signed_cas_response(&cmd,&ack,&p),Err(Error::InvalidSignature));
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn modified_signed_payload_or_status_without_resign_denied(){
         let (d,_,_,_,_,_,_,entries)=fixture();
         let c=key();let w=key();let p=pins(&c,&w);
         let cmd=command(&entries,&c,&"a6".repeat(32),&"b6".repeat(32));
         let original=receipt(&cmd,&w,ResultKind::Committed);
         let mut altered=original.clone();
         altered.receipt.result=ResultKind::AlreadyCommitted;
         assert_eq!(verify_signed_cas_response(&cmd,&altered,&p),
             Err(Error::InvalidSignature));
         let mut altered=original.clone();
         altered.receipt.latest_entry_sha256="0a".repeat(32);
         assert_eq!(verify_signed_cas_response(&cmd,&altered,&p),
             Err(Error::InvalidSignature));
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn genuinely_signed_wrong_revision_hash_genesis_or_owner_rejected(){
         let (d,_,_,_,_,_,_,entries)=fixture();
         let c=key();let w=key();let p=pins(&c,&w);
         let cmd=command(&entries,&c,&"a7".repeat(32),&"b7".repeat(32));
         for mode in 0..4{
             let mut reply=offline_receipt_frame(&cmd,ResultKind::Committed).unwrap();
             match mode{
                 0=>reply.latest_committed_revision=3,
                 1=>reply.latest_entry_sha256="de".repeat(32),
                 2=>reply.immutable_genesis_sha256="ba".repeat(32),
                 _=>reply.original_owner_generation+=1,
             }
             let signed_reply=SignedReceipt{
                 witness_ed25519_signature_hex:signed(&w,&cas_receipt_message(&reply).unwrap()),
                 receipt:reply,
             };
             assert_eq!(verify_signed_cas_response(&cmd,&signed_reply,&p),
                 Err(Error::DeniedOrUnverifiableCommit));
         }
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn rejected_cas_reply_can_never_be_interpreted_as_commit(){
         let (d,_,_,_,_,_,_,entries)=fixture();
         let c=key();let w=key();let p=pins(&c,&w);
         let cmd=command(&entries,&c,&"a8".repeat(32),&"b8".repeat(32));
         let rejected=receipt(&cmd,&w,ResultKind::Rejected);
         assert_eq!(verify_signed_cas_response(&cmd,&rejected,&p),
             Err(Error::DeniedOrUnverifiableCommit));
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn missing_public_key_or_foreign_signer_identity_fail_closed(){
         let (d,_,_,_,_,_,_,entries)=fixture();
         let c=key();let w=key();let cmd=command(&entries,&c,&"a9".repeat(32),&"b9".repeat(32));
         let reply=receipt(&cmd,&w,ResultKind::Committed);
         let mut p=pins(&c,&w);
         p.client_public_key_hex.clear();
         assert_eq!(verify_signed_cas_command(&cmd,&p),
             Err(Error::MissingIndependentPin));
         p=pins(&c,&w);p.witness_public_key_hex.clear();
         assert_eq!(verify_signed_cas_response(&cmd,&reply,&p),
             Err(Error::MissingIndependentPin));
         p=pins(&c,&w);p.witness_id="foreign-witness".into();
         assert_eq!(verify_signed_cas_command(&cmd,&p),Err(Error::WrongIdentity));
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn predecessor_gap_or_bad_hash_makes_command_invalid_before_write(){
         let (d,_,_,_,_,_,store,entries)=fixture();
         commit_all(&store,&entries[..3]);
         let c=key();let w=key();let p=pins(&c,&w);
         for mode in 0..4{
             let mut cmd=command(&entries,&c,&"aa".repeat(32),&"bb".repeat(32));
             match mode{
                 0=>cmd.request.expected_predecessor_revision=1,
                 1=>cmd.request.expected_predecessor_entry_sha256=Some("0f".repeat(32)),
                 2=>cmd.request.candidate.previous_entry_sha256="1f".repeat(32),
                 _=>cmd.request.domain="UNKNOWN-DOMAIN".into(),
             }
             assert!(cas_request_message(&cmd.request).is_err());
             assert!(verify_signed_cas_command(&cmd,&p).is_err());
             assert_eq!(store.latest_offline().unwrap().unwrap().revision,3);
         }
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn signature_cannot_be_transplanted_to_changed_cas_candidate(){
         let (d,_,_,_,_,_,_,entries)=fixture();
         let c=key();let w=key();let p=pins(&c,&w);
         let mut cmd=command(&entries,&c,&"ab".repeat(32),&"bc".repeat(32));
         cmd.request.candidate.owner_state_sha256="de".repeat(32);
         assert_eq!(verify_signed_cas_command(&cmd,&p),Err(Error::InvalidSignature));
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn ambiguous_lost_ack_still_requires_authenticated_fresh_read(){
         let (d,_,_,_,_,_,store,entries)=fixture();
         commit_all(&store,&entries[..3]);
         let c=key();let w=key();let p=pins(&c,&w);
         let cmd=command(&entries,&c,&"ac".repeat(32),&"bd".repeat(32));
         assert!(store.commit_offline(&entries[3],Wfp::AfterRename).is_err());
         // Local current state is not a remotely authenticated ACK.
         assert_eq!(store.latest_offline().unwrap().unwrap(),entries[3]);
         let fabricated_reply=receipt(&cmd,&w,ResultKind::AlreadyCommitted);
         let r=verify_signed_cas_response(&cmd,&fabricated_reply,&p).unwrap();
         assert!(!r.independently_durable_remote_commit_proven);
         assert!(!r.trading_authorized);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn mock_endpoint_rejects_wrong_signer_and_does_not_touch_durable_tip(){
         let (d,_,_,_,_,_,store,entries)=fixture();
         commit_all(&store,&entries[..3]);
         let c=key();let w=key();let impostor=key();
         let cmd=command(&entries,&c,&"ad".repeat(32),&"be".repeat(32));
         let bad_pin=pins(&impostor,&w);
         assert!(store.accept_signed_cas_offline(&cmd,&bad_pin,Wfp::None).is_err());
         assert_eq!(store.latest_offline().unwrap().unwrap().revision,3);
         fs::remove_dir_all(d).unwrap();
     }
     #[test]
     fn mock_endpoint_rejects_stale_expected_predecessor_after_newer_cas(){
         let (d,_,_,_,_,_,store,entries)=fixture();
         commit_all(&store,&entries[..3]);
         let c=key();let w=key();let p=pins(&c,&w);
         let cmd=command(&entries,&c,&"ae".repeat(32),&"bf".repeat(32));
         store.accept_signed_cas_offline(&cmd,&p,Wfp::None).unwrap();
         let mut stale=cmd.clone();
         stale.request.expected_predecessor_revision=2;
         assert!(store.accept_signed_cas_offline(&stale,&p,Wfp::None).is_err());
         assert_eq!(store.latest_offline().unwrap().unwrap().revision,4);
         fs::remove_dir_all(d).unwrap();
     }
 }

// R209: reuse the pre-existing immutable first-leg Case, not a new mock
// approximation of the OKX fill/bill/feeCcy owner contract.
mod r209_persisted_okx_fee_day_tests{
 use super::*;
 use spotarb_native_market_core::{
  native_okx_fee_checkpoint_bridge_r209_review::{
   inspect_v2_receipts_okx_fee_day_r209,V2FeeReceiptInputR209,
   V2FeeDayBridgeError},
  native_okx_fee_day_r208_review::OkxFeeDayError,
  native_trade2f3_first_leg_v2_checkpoint::{
   prepare_first_leg_v2,persist_v2_once,CheckpointFailpoint},
  native_daily_risk::{yekt_day_index,UTC_DAY_MS,YEKT_OFFSET_MS},
 };
 use std::sync::atomic::{AtomicUsize,Ordering};
 static ID:AtomicUsize=AtomicUsize::new(0);
 fn store(c:&Case,generation:u64)->std::path::PathBuf{
  let n=ID.fetch_add(1,Ordering::Relaxed);
  let dir=std::env::temp_dir().join(format!(
   "astra_r209_fee_day_{}_{}",std::process::id(),n));
  std::fs::create_dir(&dir).unwrap();
  let path=dir.join("original_v2.json");
  let p=prepare_first_leg_v2(
    generation,&c.policy,&c.plan,&c.first,&c.source).unwrap();
  persist_v2_once(&path,&p,CheckpointFailpoint::None).unwrap();
  path
 }
 fn clean(path:&std::path::Path){std::fs::remove_dir_all(path.parent().unwrap()).unwrap();}
 #[test]fn r209_persisted_quote_fee_retains_original_execution_day(){
  let c=seeded(false);let path=store(&c,99);
  let cfg=NativeTradingConfig::parse(CONFIG).unwrap();
  let req=[V2FeeReceiptInputR209{path:&path,original_generation:99,
   policy:&c.policy,plan:&c.plan,first:&c.first,
   account_owner:"original-review-owner"}];
  let when=c.source[0].at_ms+30_000;
  let report=inspect_v2_receipts_okx_fee_day_r209(
    &cfg,when,&req,&["spot-status-unknown".into()]).unwrap();
  assert_eq!(report.known_today_fill_count,1);
  assert_eq!(report.known_quote_okx_fees_e12,a("0.009").unwrap());
  assert_eq!(report.open_or_unknown_order_count,1);
  assert!(!report.complete_day_fee_history_proven);
  assert!(!report.below_cap_authoritatively_proven);
  assert!(!report.exchange_source_authenticated&&!report.may_open_real);
  let after=(yekt_day_index(when).unwrap()+1)*UTC_DAY_MS
    -YEKT_OFFSET_MS+1;
  let future_day=inspect_v2_receipts_okx_fee_day_r209(
    &cfg,after,&req,&[]).unwrap();
  assert_eq!(future_day.known_today_fill_count,0);
  assert_eq!(future_day.known_earlier_fill_count,1);
  clean(&path);
 }
 #[test]fn r209_base_fee_cannot_invent_a_quote_commission(){
  let c=seeded(true);let path=store(&c,99);
  let cfg=NativeTradingConfig::parse(CONFIG).unwrap();
  let req=[V2FeeReceiptInputR209{path:&path,original_generation:99,
   policy:&c.policy,plan:&c.plan,first:&c.first,
   account_owner:"original-review-owner"}];
  assert_eq!(inspect_v2_receipts_okx_fee_day_r209(
    &cfg,c.source[0].at_ms+10_000,&req,&[]),
    Err(V2FeeDayBridgeError::Unsupported));
  clean(&path);
 }
 #[test]fn r209_replayed_trade_between_local_checkpoints_denied(){
  let c=seeded(false);let a_path=store(&c,99);
  let b_path=store(&c,100);
  let cfg=NativeTradingConfig::parse(CONFIG).unwrap();
  let req=[
   V2FeeReceiptInputR209{path:&a_path,original_generation:99,
    policy:&c.policy,plan:&c.plan,first:&c.first,
    account_owner:"original-review-owner"},
   V2FeeReceiptInputR209{path:&b_path,original_generation:100,
    policy:&c.policy,plan:&c.plan,first:&c.first,
    account_owner:"original-review-owner"}
  ];
  assert_eq!(inspect_v2_receipts_okx_fee_day_r209(
    &cfg,c.source[0].at_ms+10_000,&req,&[]),
    Err(V2FeeDayBridgeError::Fee(OkxFeeDayError::Duplicate)));
  clean(&a_path);clean(&b_path);
 }
 #[test]fn r209_wrong_generation_policy_or_unacknowledged_file_denied(){
  let c=seeded(false);let path=store(&c,99);
  let cfg=NativeTradingConfig::parse(CONFIG).unwrap();
  let mut req=[V2FeeReceiptInputR209{path:&path,original_generation:98,
   policy:&c.policy,plan:&c.plan,first:&c.first,
   account_owner:"original-review-owner"}];
  let now=c.source[0].at_ms+10_000;
  assert_eq!(inspect_v2_receipts_okx_fee_day_r209(
    &cfg,now,&req,&[]),Err(V2FeeDayBridgeError::Original));
  req[0].original_generation=99;
  let mut wrong_cfg=cfg.clone();
  wrong_cfg.policy.daily_okx_fee_cap_e18=1;
  assert_eq!(inspect_v2_receipts_okx_fee_day_r209(
    &wrong_cfg,now,&req,&[]),Err(V2FeeDayBridgeError::Config));
  let marker=std::path::PathBuf::from(
   format!("{}.incomplete",path.display()));
  std::fs::write(&marker,b"unfinished original").unwrap();
  assert_eq!(inspect_v2_receipts_okx_fee_day_r209(
    &cfg,now,&req,&[]),Err(V2FeeDayBridgeError::Original));
  std::fs::remove_file(marker).unwrap();
  let mut bytes=std::fs::read(&path).unwrap();
  bytes[10]^=1;
  std::fs::write(&path,bytes).unwrap();
  assert!(inspect_v2_receipts_okx_fee_day_r209(
    &cfg,now,&req,&[]).is_err());
  clean(&path);
 }
}
