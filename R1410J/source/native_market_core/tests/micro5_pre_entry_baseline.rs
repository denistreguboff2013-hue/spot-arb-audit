//! MICRO5A: ordinary-linked fake signed GET of PRE-ENTRY Binance wallet.
//! No real credentials, network, order submission or Native Hedged release.
use spotarb_native_market_core::{
    authoritative_state::{
        NativeAuthoritativeState,NativeStateStore,NativeEntryPlan,NativeEntryPhase,
    },
    native_pre_entry_binance_baseline::PreEntryBinanceWrite,
    private_https::{HttpExecutor,HttpsRequest,HttpResponse,TransportError},
    private_rest::{BinanceCredentials,HttpMethod},
    fill_economics::{
        PositionEconomicsState,EntryEconomicsState,EntryTiming,LegFillAggregate,decimal_to_atoms,
    },
};
use std::sync::atomic::{AtomicUsize,Ordering};
use std::path::PathBuf;

fn root(tag:&str)->PathBuf{
    let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .unwrap().as_nanos();
    std::env::temp_dir().join(format!("micro5a-{tag}-{}-{n}",std::process::id()))
}
fn empty(tag:&str)->(PathBuf,NativeStateStore){
    let dir=root(tag);
    let store=NativeStateStore::open(dir.join("state.json")).unwrap();
    store.create(&NativeAuthoritativeState::empty_from_legacy("11".repeat(32),0)).unwrap();
    (dir,store)
}
fn now()->u64{spotarb_native_market_core::private_https::wall_ms()}
#[derive(Default)]
struct FakeBinance{
    calls:AtomicUsize,
    balance_reads:AtomicUsize,
    defect:usize,
    first_read_ms:u64,
}
impl FakeBinance{
    fn new(defect:usize,at:u64)->Self{
        Self{calls:AtomicUsize::new(0),balance_reads:AtomicUsize::new(0),
            defect,first_read_ms:at.saturating_sub(1_000)}
    }
    fn total(&self)->usize{self.calls.load(Ordering::SeqCst)}
}
impl HttpExecutor for FakeBinance{
    fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
        assert_eq!(req.method,HttpMethod::Get);
        assert!(!req.mutation);
        let u=url::Url::parse(&req.url).unwrap();
        assert_eq!(u.host_str(),Some("fapi.binance.com"));
        self.calls.fetch_add(1,Ordering::SeqCst);
        let body=match u.path(){
            "/fapi/v1/positionSide/dual"=>{
                if self.defect==10{r#"{"dualSidePosition":true}"#.into()}
                else{r#"{"dualSidePosition":false}"#.into()}
            },
            "/fapi/v1/accountConfig"=>{
                if self.defect==4{r#"{"multiAssetsMargin":true}"#.into()}
                else{r#"{"multiAssetsMargin":false}"#.into()}
            },
            "/fapi/v1/openOrders"=>{
                if self.defect==2{
                    r#"[{"symbol":"BNTUSDT","orderId":9,"clientOrderId":"foreign","side":"SELL","positionSide":"BOTH","status":"NEW","reduceOnly":false}]"#.into()
                }else{"[]".into()}
            },
            "/fapi/v3/positionRisk"=>{
                if self.defect==3{
                    r#"[{"symbol":"BNTUSDT","positionAmt":"-0.4","positionSide":"BOTH"}]"#.into()
                }else if self.defect==9{
                    r#"[{"symbol":"BNTUSDT","positionAmt":"0","positionSide":"SHORT"}]"#.into()
                }else{
                    r#"[{"symbol":"BNTUSDT","positionAmt":"0.000","positionSide":"BOTH"}]"#.into()
                }
            },
            "/fapi/v3/balance"=>{
                let index=self.balance_reads.fetch_add(1,Ordering::SeqCst);
                if self.defect==5{"[]".into()}
                else {
                    let amount=if (self.defect==1||self.defect==6)&&index>0{
                        "99.9"
                    }else{"100"};
                    let available=if self.defect==7{"95"}else{amount};
                    let age=if self.defect==8{self.first_read_ms.saturating_sub(300_000)}
                    else{self.first_read_ms};
                    format!(r#"[{{"asset":"USDT","balance":"{amount}","availableBalance":"{available}","updateTime":{age}}}]"#)
                }
            },
            p=>panic!("MICRO5 unexpected private REST GET {p}"),
        };
        Ok(HttpResponse{status:200,headers:vec![],body})
    }
}
fn capture(
    store:&NativeStateStore,exec:&FakeBinance,generation:u64,at:u64,
)->Result<(NativeAuthoritativeState,PreEntryBinanceWrite),String>{
    let creds=BinanceCredentials::new("FAKEBINANCE","FAKESECRET").unwrap();
    store.audit_and_record_pre_entry_binance_baseline_once(
        exec,generation,&creds,at,10_000,7).map_err(|e|format!("{e:?}"))
}
fn sample_entry(created:u64,safety:u64)->(NativeEntryPlan,PositionEconomicsState){
    let target=decimal_to_atoms("1").unwrap();
    let plan=NativeEntryPlan{
        base:"BNT".into(),spot_symbol:"BNT/USDT".into(),
        futures_symbol:"BNTUSDT".into(),
        created_at_ms:created,
        planned_qty_atoms_e18:1_000_000_000_000_000_000,
        order_quote_atoms_e18:10_000_000_000_000_000_000,
        spot_baseline_atoms_e18:0,safety_generation:safety,
    };
    let econ=PositionEconomicsState{entry:EntryEconomicsState{
        signal_gross_microbps:100_000_000,
        signal_expected_net_microbps:80_000_000,
        signal_spot_price_atoms:Some(decimal_to_atoms("100").unwrap()),
        signal_futures_price_atoms:Some(decimal_to_atoms("101").unwrap()),
        projected_cost_reserve_microbps:20_000_000,
        spot:LegFillAggregate::new("okx","BNT/USDT","buy",target).unwrap(),
        futures:LegFillAggregate::new("binance","BNTUSDT","sell",target).unwrap(),
        timing:EntryTiming::default(),
        post_leg1_projected_gross_microbps:None,
        post_leg1_projected_net_microbps:None,
        locked_actual_gross_microbps:None,
        locked_actual_net_microbps:None,
        signal_to_leg1_slippage_microbps:None,
        signal_to_hedge_decay_microbps:None,
        execution_cost_vs_signal_microbps:None,
    },exit:None};
    (plan,econ)
}
#[test]
fn micro5a_two_independent_5_get_pre_entry_wallet_snapshots_saved_and_reloaded(){
    let (dir,store)=empty("good");
    let t=now();
    let fake=FakeBinance::new(0,t);
    let (saved,status)=capture(&store,&fake,1,t).unwrap();
    assert_eq!(status,PreEntryBinanceWrite::Recorded);
    assert_eq!(saved.generation,2);
    assert_eq!(fake.total(),10,"2*5 independent signed GET snapshots");
    assert!(saved.entry.is_none()&&saved.active.is_none());
    let witness=saved.pre_entry_binance_baseline.as_ref().unwrap();
    assert_eq!(witness.source_generation(),1);
    assert_eq!(witness.captured_at_ms(),t);
    assert_eq!(witness.wallet_usdt_e12(),decimal_to_atoms("100").unwrap());
    assert_eq!(witness.available_usdt_e12(),decimal_to_atoms("100").unwrap());
    assert!(!witness.may_send_order()&&!witness.history_complete());
    let reloaded=NativeStateStore::open(dir.join("state.json")).unwrap();
    assert_eq!(reloaded.load().unwrap().unwrap(),saved);
    assert!(reloaded.load().unwrap().unwrap().pre_entry_binance_baseline.is_some());
    let hits=fake.total();
    assert!(capture(&reloaded,&fake,2,t).is_err());
    assert_eq!(hits,fake.total(),"no second preentry wallet snapshot after capture");
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro5a_wallet_drift_between_snapshots_and_unsafe_account_flags_deny_without_write(){
    for (tag,mode) in [
        ("wallet-change",1),("open-order",2),("prior-short",3),
        ("multi-margin",4),("missing-wallet",5),("duplicate-altered-balance",6),
        ("funds-reserved",7),("wallet-stale",8),
        ("hedge-side",9),("hedge-mode",10),
    ]{
        let (dir,store)=empty(tag);
        let before=std::fs::read(dir.join("state.json")).unwrap();
        let t=now();let fake=FakeBinance::new(mode,t);
        assert!(capture(&store,&fake,1,t).is_err(),"fake unsafe state {tag} accepted");
        assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
        assert!(store.load().unwrap().unwrap().pre_entry_binance_baseline.is_none());
        assert!(fake.total()<=10,"preentry audit exceeded bounded reads");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
#[test]
fn micro5a_baseline_must_precede_first_native_entry_with_exact_safety_and_time(){
    let (dir,store)=empty("entry");
    let t=now();let fake=FakeBinance::new(0,t);
    let (saved,_)=capture(&store,&fake,1,t).unwrap();
    for (tag,start,safety) in [
        ("older",t-1,7),("expired",t+60_001,7),("wrong-safety",t+1,8),
    ]{
        let (plan,econ)=sample_entry(start,safety);
        assert!(store.begin_native_entry(saved.generation,plan,econ,tag,tag).is_err(),
            "{tag} bypassed pre-entry Binance wallet seal");
        assert_eq!(store.load().unwrap().unwrap(),saved);
    }
    let (plan,econ)=sample_entry(t+1,7);
    let (entry,_)=store.begin_native_entry(saved.generation,plan,econ,
        "micro5-spot","micro5-spot-client").unwrap();
    assert_eq!(entry.generation,3);
    assert_eq!(entry.entry.as_ref().unwrap().phase,NativeEntryPhase::SpotPrepared);
    assert_eq!(entry.pre_entry_binance_baseline,saved.pre_entry_binance_baseline);
    assert!(entry.active.is_none());
    assert!(capture(&store,&fake,entry.generation,t+1).is_err());
    assert_eq!(fake.total(),10);
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro5a_owner_cas_prevents_erasing_replacing_wallet_and_false_trading_authority(){
    let (dir,store)=empty("immutability");
    let t=now();let fake=FakeBinance::new(0,t);
    let (original,_)=capture(&store,&fake,1,t).unwrap();
    let bytes=std::fs::read(dir.join("state.json")).unwrap();
    let mut dropit=original.clone();dropit.pre_entry_binance_baseline=None;dropit.generation=3;
    assert!(store.replace(2,&dropit).is_err());
    for (case,from,to) in [
        ("fake-wallet","\"usdt_wallet_e12\":100000000000000",
            "\"usdt_wallet_e12\":99999999999999"),
        ("fake-available","\"usdt_available_e12\":100000000000000",
            "\"usdt_available_e12\":1"),
        ("fake-history","\"entire_prior_account_history_proven\":false",
            "\"entire_prior_account_history_proven\":true"),
        ("fake-rollback","\"external_anti_rollback_proven\":false",
            "\"external_anti_rollback_proven\":true"),
        ("fake-send","\"may_send_order\":false","\"may_send_order\":true"),
        ("fake-funding","\"may_assume_funding_history_complete\":false",
            "\"may_assume_funding_history_complete\":true"),
        ("fake-gen","\"source_generation\":1","\"source_generation\":2"),
    ]{
        let mut altered=original.clone();
        let raw=serde_json::to_string(&altered.pre_entry_binance_baseline).unwrap();
        let changed=raw.replacen(from,to,1);
        assert_ne!(changed,raw,"ineffective test {case}");
        altered.pre_entry_binance_baseline=serde_json::from_str(&changed).unwrap();
        altered.generation=3;
        assert!(store.replace(2,&altered).is_err(),"{case} accepted");
        assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
    }
    let mut altered=original.clone();
    let mut original_fields=serde_json::to_value(
        altered.pre_entry_binance_baseline.as_ref().unwrap()).unwrap();
    original_fields["captured_at_ms"]=serde_json::json!(0);
    altered.pre_entry_binance_baseline=Some(serde_json::from_value(original_fields).unwrap());
    altered.generation=3;
    assert!(store.replace(2,&altered).is_err(),
        "forged capture time accepted");
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
    assert!(store.transact(2,|s|{s.pre_entry_binance_baseline=None;Ok(())}).is_err());
    assert!(store.persist_with_failpoint(&dropit,
        spotarb_native_market_core::authoritative_state::StatePersistFailpoint::None).is_err());
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro5a_stale_generation_and_disk_failure_deny_before_or_after_reads_without_partial_state(){
    let (dir,store)=empty("stale");
    let t=now();let fake=FakeBinance::new(0,t);
    assert!(capture(&store,&fake,0,t).is_err());
    assert_eq!(fake.total(),0,"stale owner must not query real accounts");
    std::fs::remove_dir_all(dir).unwrap();
    let (dir,store)=empty("disk");
    let bytes=std::fs::read(dir.join("state.json")).unwrap();
    let blocker=dir.join("state.tmp");
    std::fs::create_dir(&blocker).unwrap();
    let fake=FakeBinance::new(0,now());
    let t=now();
    assert!(capture(&store,&fake,1,t).is_err());
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
    std::fs::remove_dir(&blocker).unwrap();
    let (recorded,_)=capture(&store,&fake,1,t).unwrap();
    assert_eq!(recorded.generation,2);
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro5a_concurrent_snapshot_writers_only_one_can_persist(){
    let (dir,store)=empty("race");
    let t=now();
    let a=FakeBinance::new(0,t);let b=FakeBinance::new(0,t);
    std::thread::scope(|scope|{
        let x=scope.spawn(||capture(&store,&a,1,t));
        let y=scope.spawn(||capture(&store,&b,1,t));
        let both=[x.join().unwrap(),y.join().unwrap()];
        assert_eq!(both.iter().filter(|r|r.is_ok()).count(),1);
        assert_eq!(both.iter().filter(|r|r.is_err()).count(),1);
    });
    assert_eq!(store.load().unwrap().unwrap().generation,2);
    assert!(store.load().unwrap().unwrap().pre_entry_binance_baseline.is_some());
    std::fs::remove_dir_all(dir).unwrap();
}
