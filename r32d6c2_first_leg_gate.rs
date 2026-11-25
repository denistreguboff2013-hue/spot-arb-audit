//! R32D6C2 normally-linked integration tests: futures preparation must
//! fail closed for cumulative-only settled Spot. No external network calls.
use spotarb_native_market_core::{
    authoritative_state::{
        NativeEntryPhase,NativeEntryPlan,NativeAuthoritativeState,NativeStateStore,
    },
    fill_economics::{PositionEconomicsState,EntryEconomicsState,EntryTiming,LegFillAggregate,decimal_to_atoms},
    native_spot_prehedge::audit_okx_first_leg_before_hedge,
    native_spot_prehedge_bills::audit_okx_first_leg_cash_bills_window,
    private_https::{HttpsRequest,HttpExecutor,HttpResponse,TransportError},
    private_rest::{OkxCredentials,OkxDomain,HttpMethod},
};
use std::sync::atomic::{AtomicUsize,Ordering};
fn root(tag:&str)->std::path::PathBuf{
    let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("r32d6c2-linked-{tag}-{}-{n}",std::process::id()))
}
fn prepared()->(std::path::PathBuf,NativeStateStore,u64){
    let path=root("first-leg");
    let store=NativeStateStore::open(path.join("state.json")).unwrap();
    store.create(&NativeAuthoritativeState::empty_from_legacy("11".repeat(32),0)).unwrap();
    let one=decimal_to_atoms("1").unwrap();
    let econ=PositionEconomicsState{entry:EntryEconomicsState{
        signal_gross_microbps:100_000_000,
        signal_expected_net_microbps:80_000_000,
        signal_spot_price_atoms:Some(decimal_to_atoms("100").unwrap()),
        signal_futures_price_atoms:Some(decimal_to_atoms("101").unwrap()),
        projected_cost_reserve_microbps:20_000_000,
        spot:LegFillAggregate::new("okx","BNT/USDT","buy",one).unwrap(),
        futures:LegFillAggregate::new("binance","BNTUSDT","sell",one).unwrap(),
        timing:EntryTiming::default(),
        post_leg1_projected_gross_microbps:None,
        post_leg1_projected_net_microbps:None,
        locked_actual_gross_microbps:None,
        locked_actual_net_microbps:None,
        signal_to_leg1_slippage_microbps:None,
        signal_to_hedge_decay_microbps:None,
        execution_cost_vs_signal_microbps:None,
    },exit:None};
    let plan=NativeEntryPlan{
        base:"BNT".into(),spot_symbol:"BNT/USDT".into(),
        futures_symbol:"BNTUSDT".into(),
        created_at_ms:spotarb_native_market_core::private_https::wall_ms().saturating_sub(10_000),
        planned_qty_atoms_e18:1_000_000_000_000_000_000,
        order_quote_atoms_e18:10_000_000_000_000_000_000,
        spot_baseline_atoms_e18:125_000_000_000_000_000,
        safety_generation:7,
    };
    let (_,permit)=store.begin_native_entry(1,plan,econ,
        "r32d6c2-spot","sab00000000000000000000000000791").unwrap();
    let state=store.record_spot_entry_observation(
        permit,1_000_000_000_000_000_000,
        Some(decimal_to_atoms("100").unwrap()),true,false,false).unwrap();
    assert_eq!(state.entry.unwrap().phase,NativeEntryPhase::SpotExposed);
    (path,store,state.generation)
}
struct SyntheticOkx{calls:AtomicUsize,base_ms:u64}
impl Default for SyntheticOkx{
    fn default()->Self{
        Self{calls:AtomicUsize::new(0),
            base_ms:spotarb_native_market_core::private_https::wall_ms().saturating_sub(10_000)}
    }
}
impl HttpExecutor for SyntheticOkx{
    fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
        assert_eq!(req.method,HttpMethod::Get);
        assert!(!req.mutation);
        let url=url::Url::parse(&req.url).unwrap();
        let query=url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
        self.calls.fetch_add(1,Ordering::SeqCst);
        let body=match url.path(){
            "/api/v5/account/balance"=>
                format!(r#"{{"code":"0","data":[{{"details":[{{"ccy":"BNT","cashBal":"1.125","availBal":"1.125","frozenBal":"0","uTime":"{}"}}]}}]}}"#,self.base_ms+2000),
            "/api/v5/trade/order"=>{
                assert_eq!(query.get("clOrdId").map(|v|v.as_ref()),
                    Some("sab00000000000000000000000000791"));
                format!(r#"{{"code":"0","data":[{{"ordId":"789","clOrdId":"sab00000000000000000000000000791","state":"filled","accFillSz":"1","avgPx":"100","fillTime":"{}"}}]}}"#,self.base_ms+320)
            },
            "/api/v5/trade/fills-history"=>{
                assert_eq!(query.get("ordId").map(|v|v.as_ref()),Some("789"));
                if query.get("after").is_some(){
                    r#"{"code":"0","data":[]}"#.into()
                }else{
                    format!(r#"{{"code":"0","data":[{{"ordId":"789","billId":"301","tradeId":"T1","fillPx":"100","fillSz":"1","fee":"-0.1","feeCcy":"USDT","fillTime":"{}"}}]}}"#,self.base_ms+320)
                }
            },
            "/api/v5/account/bills-archive"=>{
                assert_eq!(query.get("ccy").map(|v|v.as_ref()),Some("BNT"));
                let begin=query.get("begin").unwrap().parse::<u64>().unwrap();
                let end=query.get("end").unwrap().parse::<u64>().unwrap();
                assert!(begin>0 && end>=begin && end-begin<30_000);
                assert!(!query.contains_key("instType"));
                if query.get("after").is_some(){
                    r#"{"code":"0","data":[]}"#.into()
                }else{
                    format!(r#"{{"code":"0","data":[{{"ccy":"BNT","ordId":"789","instId":"BNT-USDT","subType":"1","tradeId":"T1","billId":"301","bal":"1.125","balChg":"1","sz":"1","fee":"-0.1","feeCcy":"USDT","ts":"{}"}}]}}"#,self.base_ms+350)
                }
            },
            "/api/v5/account/config"=>r#"{"code":"0","data":[{"acctLv":"2","posMode":"net_mode"}]}"#.into(),
            "/api/v5/account/positions"=>r#"{"code":"0","data":[]}"#.into(),
            "/api/v5/trade/orders-pending"=>r#"{"code":"0","data":[]}"#.into(),
            p=>panic!("unexpected URL path {p}"),
        };
        Ok(HttpResponse{status:200,headers:vec![],body})
    }
}
#[test]
fn r32d6c2_normal_build_refuses_futures_hedge_before_capture(){
    let (root,store,generation)=prepared();
    let previous=std::fs::read(root.join("state.json")).unwrap();
    assert!(store.prepare_futures_entry_hedge(generation,7,
        "r32d6c2-fut","sab00000000000000000000000000792").is_err());
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),previous);
    assert!(store.load().unwrap().unwrap().entry.unwrap()
        .spot_prehedge_evidence.is_none());
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn r32d6c2_normal_build_passes_prepare_only_after_exact_audited_fsync(){
    let (root,store,generation)=prepared();
    let executor=SyntheticOkx::default();
    let creds=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let report=audit_okx_first_leg_before_hedge(
        &executor,&store,generation,&creds,spotarb_native_market_core::private_https::wall_ms(),OkxDomain::Global,1).unwrap();
    assert_eq!(executor.calls.load(Ordering::SeqCst),5);
    assert!(!report.may_send_order);
    let (saved,_) =store.record_native_spot_first_leg_evidence(report).unwrap();
    let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
    assert_eq!(reopened.load().unwrap().unwrap(),saved);
    let raw=std::fs::read(root.join("state.json")).unwrap();
    assert!(reopened.prepare_futures_entry_hedge(
        saved.generation,7,"r32d6c2-fut","sab00000000000000000000000000792").is_err());
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),raw);
    let cash_report=audit_okx_first_leg_cash_bills_window(
        &executor,&reopened,saved.generation,&creds,
        spotarb_native_market_core::private_https::wall_ms(),OkxDomain::Global,1).unwrap();
    assert_eq!(executor.calls.load(Ordering::SeqCst),9);
    assert_eq!(cash_report.rows,1);
    assert!(cash_report.window_matched);
    assert!(!cash_report.may_send_order);
    let (bills_record,_) =reopened.record_native_spot_prehedge_bills_evidence(cash_report).unwrap();
    assert_eq!(bills_record.generation,saved.generation+1);
    assert!(bills_record.entry.as_ref().unwrap().spot_prehedge_bills_evidence.is_some());
    let (future,_) =reopened.prepare_futures_entry_hedge(
        bills_record.generation,7,"r32d6c2-fut",
        "sab00000000000000000000000000792").unwrap();
    assert_eq!(future.entry.as_ref().unwrap().phase,NativeEntryPhase::FuturesPrepared);
    assert_eq!(future.entry.as_ref().unwrap()
        .spot_prehedge_evidence.as_ref().unwrap().exact_ledger.fills.len(),1);
    assert_eq!(future.entry.unwrap().futures_filled_atoms_e18,0);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn r32d6c4_ordinary_state_validator_rejects_synthetic_native_active_all_write_apis(){
    use spotarb_native_market_core::{
        authoritative_state::{NativePositionBundle,NativePositionCore,PositionProvenance,StatePersistFailpoint},
        position_lifecycle::PositionLifecycle,
    };
    let target=1_000_000_000_000_000_000u128;
    let core=NativePositionCore{
        base:"BNT".into(),spot_symbol:"BNT/USDT".into(),futures_symbol:"BNTUSDT".into(),
        opened_at_ms:spotarb_native_market_core::private_https::wall_ms(),
        spot_baseline_atoms_e18:125_000_000_000_000_000,
        spot_net_atoms_e18:target,futures_target_atoms_e18:target,
        order_quote_atoms_e18:100_000_000_000_000_000_000,
    };
    let bundle=NativePositionBundle{
        core,lifecycle:PositionLifecycle::new_hedged(target,7).unwrap(),
        intents:vec![],economics:None,provenance:PositionProvenance::Native,
        reconciliation_required:false,
        origin:None,
    };
    // This position is internally coherent but has NO independent exchange
    // history, so a caller must not synthesize a native Hedged authority.
    bundle.validate().unwrap();
    let dir=root("r32d6c4-forged-active");
    let store=NativeStateStore::open(dir.join("state.json")).unwrap();
    let mut spoof=NativeAuthoritativeState::empty_from_legacy("11".repeat(32),0);
    spoof.active=Some(bundle.clone());
    assert!(spoof.validate().is_err());
    assert!(store.create(&spoof).is_err());
    assert!(!dir.join("state.json").exists());
    store.create(&NativeAuthoritativeState::empty_from_legacy("11".repeat(32),0)).unwrap();
    let raw=std::fs::read(dir.join("state.json")).unwrap();
    assert!(store.transact(1,|state|{
        state.active=Some(bundle.clone());
        Ok(())
    }).is_err());
    let mut next=spoof.clone();
    next.generation=2;
    assert!(store.replace(1,&next).is_err());
    assert!(store.persist_with_failpoint(&next,StatePersistFailpoint::None).is_err());
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),raw);
    let previous=store.load_for_recovery().unwrap().unwrap();
    assert!(previous.active.is_none());
    assert_eq!(previous.generation,1);
    std::fs::remove_dir_all(dir).unwrap();
}

fn r32d6c4_proven_spot_bills_before_futures()->(std::path::PathBuf,NativeStateStore,u64){
    let (dir,store,gen)=prepared();
    let executor=SyntheticOkx::default();
    let creds=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let proof=audit_okx_first_leg_before_hedge(
        &executor,&store,gen,&creds,
        spotarb_native_market_core::private_https::wall_ms(),OkxDomain::Global,1).unwrap();
    let (first,_) =store.record_native_spot_first_leg_evidence(proof).unwrap();
    let bills=audit_okx_first_leg_cash_bills_window(
        &executor,&store,first.generation,&creds,
        spotarb_native_market_core::private_https::wall_ms(),OkxDomain::Global,1).unwrap();
    let (both,_) =store.record_native_spot_prehedge_bills_evidence(bills).unwrap();
    (dir,store,both.generation)
}

#[test]
fn r32d6c4_recent_receipts_expire_and_future_clock_fails_closed(){
    let (dir,store,generation)=r32d6c4_proven_spot_bills_before_futures();
    let state=store.load().unwrap().unwrap();
    let e=state.entry.as_ref().unwrap();
    let checked=e.spot_prehedge_bills_evidence.as_ref().unwrap().observed_at_ms;
    e.require_recent_spot_bills_provenance(checked).unwrap();
    assert!(e.require_recent_spot_bills_provenance(checked+60_001).is_err());
    assert!(e.require_recent_spot_bills_provenance(checked-5_001).is_err());
    assert_eq!(state.generation,generation);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn r32d6c4_signed_cumulative_futures_full_does_not_mint_hedged_authority(){
    let (dir,store,generation)=r32d6c4_proven_spot_bills_before_futures();
    assert!(store.prepare_futures_entry_hedge(generation,8,
        "r32d6c4-stale","sab00000000000000000000000000820").is_err());
    let (prepared,permit)=store.prepare_futures_entry_hedge(
        generation,7,"r32d6c4-futures","sab00000000000000000000000000821").unwrap();
    assert_eq!(prepared.entry.as_ref().unwrap().phase,NativeEntryPhase::FuturesPrepared);
    let bytes=std::fs::read(dir.join("state.json")).unwrap();
    let result=store.record_futures_entry_observation(
        permit,1_000_000_000_000_000_000,
        Some(decimal_to_atoms("101").unwrap()),true,false,false);
    assert!(result.is_err());
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
    let unchanged=store.load().unwrap().unwrap();
    assert_eq!(unchanged.generation,prepared.generation);
    assert!(unchanged.active.is_none());
    assert_eq!(unchanged.entry.as_ref().unwrap().phase,NativeEntryPhase::FuturesPrepared);
    // Restart is even stricter: a prepared-but-unresolved intent is
    // surfaced as RecoveryRequired, never as a fresh send permission.
    let recovered=store.load_for_recovery().unwrap().unwrap();
    assert_eq!(recovered.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
    assert!(unchanged.entry.as_ref().unwrap().spot_prehedge_bills_evidence.is_some());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn r32d6c5_normal_build_rejects_partial_and_full_aggregate_futures_ack(){
    // Native permits are deliberately non-Clone; use two independently
    // prepared durable transactions to test partial and full cumulative ACK.
    for qty in [200_000_000_000_000_000u128,1_000_000_000_000_000_000u128]{
        let (root,store,generation)=r32d6c4_proven_spot_bills_before_futures();
        let (state,permit)=store.prepare_futures_entry_hedge(
            generation,7,"r32d6c5-futures",
            "sab00000000000000000000000000861").unwrap();
        let original=std::fs::read(root.join("state.json")).unwrap();
        assert!(store.record_futures_entry_observation(
            permit,qty,Some(decimal_to_atoms("101").unwrap()),
            true,false,false,
        ).is_err());
        assert_eq!(std::fs::read(root.join("state.json")).unwrap(),original);
        assert_eq!(store.load().unwrap().unwrap().generation,state.generation);
        assert!(store.load().unwrap().unwrap().active.is_none());
        std::fs::remove_dir_all(root).unwrap();
    }
    let (root,store,generation)=r32d6c4_proven_spot_bills_before_futures();
    let (_,permit)=store.prepare_futures_entry_hedge(
        generation,7,"r32d6c5-futures",
        "sab00000000000000000000000000861").unwrap();
    // UNKNOWN preserves exact original intent and requires recovery.
    let recovered=store.record_futures_entry_observation(
        permit,0,None,false,false,true,
    ).unwrap();
    assert_eq!(recovered.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
    assert!(recovered.active.is_none());
    assert!(recovered.entry.as_ref().unwrap().futures_entry_window_evidence.is_none());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn r32d6c6_normal_build_unknown_futures_without_checkpoint_no_external_get(){
    use spotarb_native_market_core::{
        native_prospective_joint::{audit_prospective_joint_chain_once,ProspectiveJointError},
        private_rest::{BinanceCredentials},
    };
    let (root,store,generation)=r32d6c4_proven_spot_bills_before_futures();
    let (_,permit)=store.prepare_futures_entry_hedge(
        generation,7,"r32d6c6-futures",
        "sab00000000000000000000000000891").unwrap();
    let unknown=store.record_futures_entry_observation(
        permit,0,None,false,false,true).unwrap();
    let prior=std::fs::read(root.join("state.json")).unwrap();
    let executor=SyntheticOkx::default();
    let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    let result=audit_prospective_joint_chain_once(
        &executor,&store,unknown.generation,&okx,&bin,
        spotarb_native_market_core::private_https::wall_ms(),
        OkxDomain::Global,10_000,1,10);
    assert!(matches!(result,Err(ProspectiveJointError::Invalid(_))));
    assert_eq!(executor.calls.load(Ordering::SeqCst),0);
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),prior);
    assert!(store.load().unwrap().unwrap().active.is_none());
    let recovered=store.load_for_recovery().unwrap().unwrap();
    assert_eq!(recovered.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
    assert!(recovered.entry.as_ref().unwrap().prospective_joint_evidence.is_none());
    std::fs::remove_dir_all(root).unwrap();
}


/// R32D6C7 independently compiled, ordinary library fixture.
/// It intentionally shares no #[cfg(test)] internal helper or bypasses.
#[derive(Default)]
struct C7Faults{
    mode:std::sync::atomic::AtomicUsize,
}
struct C7LinkedExchange{
    spot:SyntheticOkx,
    client:String,
    calls:AtomicUsize,
    mutations:AtomicUsize,
    faults:C7Faults,
}
impl C7LinkedExchange{
    fn new(base_ms:u64,client:&str)->Self{
        Self{
            spot:SyntheticOkx{calls:AtomicUsize::new(0),base_ms},
            client:client.into(),
            calls:AtomicUsize::new(0),mutations:AtomicUsize::new(0),
            faults:C7Faults::default(),
        }
    }
    fn total(&self)->usize{self.calls.load(Ordering::SeqCst)}
    fn fault(&self,value:usize){self.faults.mode.store(value,Ordering::SeqCst)}
}
impl HttpExecutor for C7LinkedExchange{
    fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
        assert_eq!(req.method,HttpMethod::Get,"C7 forbids exchange mutation");
        assert!(!req.mutation,"C7 forbids mutation=true");
        self.calls.fetch_add(1,Ordering::SeqCst);
        let parsed=url::Url::parse(&req.url).unwrap();
        if parsed.path().starts_with("/api/v5/"){
            if parsed.path()=="/api/v5/account/balance" && self.faults.mode.load(Ordering::SeqCst)==7{
                // A cash-only response has no proven available inventory.
                return Ok(HttpResponse{status:200,headers:vec![],
                    body:format!(r#"{{"code":"0","data":[{{"details":[{{"ccy":"BNT","cashBal":"1.125","uTime":"{}"}}]}}]}}"#,self.spot.base_ms+2000)});
            }
            if parsed.path()=="/api/v5/account/balance" && self.faults.mode.load(Ordering::SeqCst)==6{
                // Total cash unchanged, but 1 BNT has become frozen.
                return Ok(HttpResponse{status:200,headers:vec![],
                    body:format!(r#"{{"code":"0","data":[{{"details":[{{"ccy":"BNT","cashBal":"1.125","availBal":"0.125","frozenBal":"1","uTime":"{}"}}]}}]}}"#,self.spot.base_ms+2000)});
            }
            if parsed.path()=="/api/v5/account/balance" && self.faults.mode.load(Ordering::SeqCst)==1{
                // A changed *cash amount* with no movement in original
                // durable currency-wide account-bill receipt.
                return Ok(HttpResponse{status:200,headers:vec![],
                    body:format!(r#"{{"code":"0","data":[{{"details":[{{"ccy":"BNT","cashBal":"1.225","uTime":"{}"}}]}}]}}"#,self.spot.base_ms+2000)});
            }
            return self.spot.execute_request(req)
        }
        let query=parsed.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
        let first=self.spot.base_ms;
        let fault=self.faults.mode.load(Ordering::SeqCst);
        let body=match parsed.path(){
            "/fapi/v1/order"=>{
                assert_eq!(query.get("origClientOrderId").map(|v|v.as_ref()),Some(self.client.as_str()));
                format!(r#"{{"orderId":124,"clientOrderId":"{}","status":"CANCELED","executedQty":"0.4","avgPrice":"101","cumQuote":"40.4"}}"#,self.client)
            },
            "/fapi/v1/positionSide/dual"=>r#"{"dualSidePosition":false}"#.into(),
            "/fapi/v1/accountConfig"=>r#"{"multiAssetsMargin":false}"#.into(),
            "/fapi/v1/openOrders"=>r#"[]"#.into(),
            "/fapi/v3/positionRisk"=>
                format!(r#"[{{"symbol":"BNTUSDT","positionAmt":"-0.4","positionSide":"BOTH","marginAsset":"USDT","updateTime":{}}}]"#,first+400),
            "/fapi/v3/balance"=>{
                let balance=if fault==2{"99.9"}else{"100"};
                format!(r#"[{{"asset":"USDT","balance":"{balance}","availableBalance":"99","updateTime":{}}}]"#,first+600)
            },
            "/fapi/v1/income"=>{
                assert_eq!(query.get("limit").map(|v|v.as_ref()),Some("1"));
                assert!(!query.contains_key("symbol"));
                assert!(!query.contains_key("incomeType"));
                let page=query.get("page").unwrap().parse::<u64>().unwrap();
                if page==3{r#"[]"#.into()}else{
                    let trade=1000+page;
                    let tran=5000+page;
                    let time=first+120+page*100;
                    let kind=if fault==4 && page==2{"FUNDING_FEE"}else{"COMMISSION"};
                    format!(r#"[{{"symbol":"BNTUSDT","incomeType":"{kind}","income":"-0.02","asset":"USDT","time":{time},"tranId":{tran},"tradeId":"{trade}"}}]"#)
                }
            },
            "/fapi/v1/userTrades"=>{
                let filtered=query.contains_key("orderId");
                if filtered{
                    assert_eq!(query.get("orderId").map(|v|v.as_ref()),Some("124"));
                }else{
                    assert!(!query.contains_key("fromId"));
                }
                let mut rows=Vec::new();
                for idx in 0..2u64{
                    if !filtered && fault==5 && idx==1{continue}
                    let event=serde_json::json!({
                        "symbol":"BNTUSDT","id":1001+idx,"orderId":124,
                        "side":"SELL","positionSide":"BOTH","marginAsset":"USDT",
                        "buyer":false,"realizedPnl":"0","price":"101","qty":"0.2",
                        "commission":"0.02","commissionAsset":"USDT","time":first+220+idx*100,
                    });
                    rows.push(event);
                }
                if !filtered && fault==3{
                    for (idx,side) in ["BUY","SELL"].iter().enumerate(){
                        rows.push(serde_json::json!({
                            "symbol":"BNTUSDT","id":2001+idx,"orderId":999,
                            "side":side,"positionSide":"BOTH","marginAsset":"USDT",
                            "buyer":idx==0,"realizedPnl":"0","price":"101",
                            "qty":"0.1","commission":"0.01","commissionAsset":"USDT",
                            "time":first+450+idx as u64*100,
                        }));
                    }
                }
                serde_json::to_string(&rows).unwrap()
            },
            p=>panic!("C7 unexpected signed GET path {p}"),
        };
        Ok(HttpResponse{status:200,headers:vec![],body})
    }
}

fn r32d6c7_prospective_chain()->(std::path::PathBuf,NativeStateStore,C7LinkedExchange,u64){
    use spotarb_native_market_core::{
        native_entry_recovery::{probe_native_entry_readonly_once,probe_native_entry_fills_readonly_once,EntryProbeVerdict},
        authoritative_state::{NativeEntryRecoveryWrite,NativeSpotFirstLegWrite,NativeSpotBillsWrite,NativeFuturesEntryWindowWrite},
        native_futures_entry_audit::audit_futures_entry_window_once,
        private_rest::BinanceCredentials,
    };
    let (root,store,gen)=prepared();
    let base=store.load().unwrap().unwrap().entry.unwrap().plan.created_at_ms;
    let client="sab00000000000000000000000000971";
    let exec=C7LinkedExchange::new(base,client);
    let now=spotarb_native_market_core::private_https::wall_ms();
    let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    let first=audit_okx_first_leg_before_hedge(
        &exec,&store,gen,&okx,now,OkxDomain::Global,1).unwrap();
    let (one,write)=store.record_native_spot_first_leg_evidence(first).unwrap();
    assert_eq!(write,NativeSpotFirstLegWrite::Recorded);
    let bills=audit_okx_first_leg_cash_bills_window(
        &exec,&store,one.generation,&okx,now,OkxDomain::Global,1).unwrap();
    let (two,write)=store.record_native_spot_prehedge_bills_evidence(bills).unwrap();
    assert_eq!(write,NativeSpotBillsWrite::Recorded);
    let (_,permit)=store.prepare_futures_entry_hedge(
        two.generation,7,"r32d6c7-futures",client).unwrap();
    let unknown=store.record_futures_entry_observation(permit,0,None,false,false,true).unwrap();
    assert_eq!(unknown.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
    let original=std::fs::read(root.join("state.json")).unwrap();
    let candidate=probe_native_entry_readonly_once(
        &exec,&store,unknown.generation,&okx,&bin,now,OkxDomain::Global,10_000).unwrap();
    assert_eq!(candidate.verdict,EntryProbeVerdict::TerminalCandidateNeedsFillFeesAndInventoryTruth);
    let detail=probe_native_entry_fills_readonly_once(
        &exec,&store,&candidate,&okx,&bin,now,OkxDomain::Global,10_000).unwrap();
    assert_eq!(detail.fill_count,2);
    assert_eq!(detail.filled_atoms_e18,400_000_000_000_000_000);
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),original);
    let (cp,write)=store.record_native_entry_recovery_checkpoint(detail).unwrap();
    assert_eq!(write,NativeEntryRecoveryWrite::Recorded);
    assert!(cp.entry.as_ref().unwrap().spot_prehedge_bills_evidence.is_some());
    let bin_report=audit_futures_entry_window_once(
        &exec,&store,cp.generation,&bin,now,10_000,1,10).unwrap();
    assert_eq!(bin_report.trade_count,2);
    let (three,write)=store.record_native_futures_entry_window(bin_report).unwrap();
    assert_eq!(write,NativeFuturesEntryWindowWrite::Recorded);
    assert_eq!(three.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
    assert!(three.entry.as_ref().unwrap().futures_entry_window_evidence.is_some());
    assert!(three.active.is_none());
    assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
    (root,store,exec,three.generation)
}

#[test]
fn r32d6c7_normal_link_positive_exact_receipts_plus_0_6_bnt_exposure_no_send(){
    use spotarb_native_market_core::{
        native_prospective_joint::audit_prospective_joint_chain_once,
        private_rest::BinanceCredentials,
        authoritative_state::NativeProspectiveJointWrite,
    };
    let (root,store,exec,generation)=r32d6c7_prospective_chain();
    let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    let original=std::fs::read(root.join("state.json")).unwrap();
    let before=exec.total();
    let report=audit_prospective_joint_chain_once(
        &exec,&store,generation,&okx,&bin,
        spotarb_native_market_core::private_https::wall_ms(),OkxDomain::Global,
        10_000,1,10).unwrap();
    assert_eq!(exec.total()-before,29);
    assert_eq!(report.spot_fill_count,1);
    assert_eq!(report.futures_fill_count,2);
    assert_eq!(report.spot_unhedged_atoms_e18,600_000_000_000_000_000);
    assert!(!report.may_send_order);
    assert!(!report.may_promote_hedged);
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),original);
    let (saved,write)=store.record_native_prospective_joint_window(report.clone()).unwrap();
    assert_eq!(write,NativeProspectiveJointWrite::Recorded);
    assert_eq!(saved.generation,generation+1);
    assert_eq!(saved.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
    assert!(saved.active.is_none());
    let chain=saved.entry.as_ref().unwrap().prospective_joint_evidence.as_ref().unwrap();
    assert_eq!(chain.spot_trade_ids,vec!["T1"]);
    assert_eq!(chain.spot_bill_ids,vec!["301"]);
    assert_eq!(chain.futures_trade_ids,vec!["1001","1002"]);
    assert_eq!(chain.exposed_spot_unhedged_atoms_e18,600_000_000_000_000_000);
    assert!(!chain.may_send_order&&!chain.may_promote_hedged);
    assert!(!chain.exchange_global_history_complete);
    assert!(!chain.cross_venue_exchange_snapshot_atomicity_proven);
    let bytes=std::fs::read(root.join("state.json")).unwrap();
    let again=NativeStateStore::open(root.join("state.json")).unwrap();
    assert_eq!(again.load().unwrap().unwrap(),saved);
    let (same,status)=again.record_native_prospective_joint_window(report).unwrap();
    assert_eq!(status,NativeProspectiveJointWrite::Idempotent);
    assert_eq!(same,saved);
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),bytes);
    assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
    std::fs::remove_dir_all(root).unwrap();
}


#[test]
fn r32d6c7_normal_link_rechecks_drift_funding_foreign_and_missing_trades(){
    use spotarb_native_market_core::{
        native_prospective_joint::audit_prospective_joint_chain_once,
        private_rest::BinanceCredentials,
    };
    for fault in [1,2,3,4,5,6,7]{
        let (root,store,exec,generation)=r32d6c7_prospective_chain();
        let bytes=std::fs::read(root.join("state.json")).unwrap();
        exec.fault(fault);
        let initial_calls=exec.total();
        let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
        let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
        let result=audit_prospective_joint_chain_once(
            &exec,&store,generation,&okx,&bin,
            spotarb_native_market_core::private_https::wall_ms(),OkxDomain::Global,
            10_000,1,10);
        assert!(result.is_err(),"fault={fault} falsely accepted");
        assert!(exec.total()>initial_calls);
        assert_eq!(std::fs::read(root.join("state.json")).unwrap(),bytes);
        let state=store.load_for_recovery().unwrap().unwrap();
        assert!(state.active.is_none());
        assert_eq!(state.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
        assert!(state.entry.as_ref().unwrap().prospective_joint_evidence.is_none());
        assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
        std::fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn r32d6c7_normal_link_expired_source_and_stale_native_gen_zero_mutations(){
    use spotarb_native_market_core::{
        native_prospective_joint::{audit_prospective_joint_chain_once,ProspectiveJointError},
        private_rest::BinanceCredentials,
        authoritative_state::NativeStateError,
    };
    let (root,store,exec,generation)=r32d6c7_prospective_chain();
    let original=std::fs::read(root.join("state.json")).unwrap();
    let creds=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    let count=exec.total();
    assert!(matches!(audit_prospective_joint_chain_once(
        &exec,&store,generation-1,&creds,&bin,
        spotarb_native_market_core::private_https::wall_ms(),
        OkxDomain::Global,10_000,1,10),
        Err(ProspectiveJointError::State(NativeStateError::Generation))));
    let first=store.load().unwrap().unwrap().entry.unwrap().spot_prehedge_evidence.unwrap();
    let stale_time=first.observed_at_ms+60_001;
    assert!(matches!(audit_prospective_joint_chain_once(
        &exec,&store,generation,&creds,&bin,stale_time,
        OkxDomain::Global,10_000,1,10),
        Err(ProspectiveJointError::Invalid(_))));
    assert_eq!(exec.total(),count);
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),original);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn r32d6c7_normal_link_cas_conflicts_and_identical_replays_cannot_double_count(){
    use spotarb_native_market_core::{
        native_prospective_joint::audit_prospective_joint_chain_once,
        authoritative_state::{NativeProspectiveJointWrite,NativeStateError},
        private_rest::BinanceCredentials,
    };
    let (root,store,exec,generation)=r32d6c7_prospective_chain();
    let creds=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    let report=audit_prospective_joint_chain_once(
        &exec,&store,generation,&creds,&bin,
        spotarb_native_market_core::private_https::wall_ms(),OkxDomain::Global,
        10_000,1,10).unwrap();
    let mut forged=report.clone();
    // The raw report is diagnostic and cannot acquire a release flag.
    forged.may_promote_hedged=true;
    assert!(store.record_native_prospective_joint_window(forged).is_err());
    let old=std::fs::read(root.join("state.json")).unwrap();
    let a=store.clone();
    let b=store.clone();
    let duplicate=report.clone();
    let first=std::thread::spawn(move||a.record_native_prospective_joint_window(report));
    let second=std::thread::spawn(move||b.record_native_prospective_joint_window(duplicate));
    let outcomes=[first.join().unwrap(),second.join().unwrap()];
    assert_eq!(outcomes.iter().filter(|r|matches!(
        r,Ok((_,NativeProspectiveJointWrite::Recorded)))).count(),1);
    assert_eq!(outcomes.iter().filter(|r|matches!(
        r,Ok((_,NativeProspectiveJointWrite::Idempotent)))).count(),1);
    let state=store.load().unwrap().unwrap();
    assert_eq!(state.generation,generation+1);
    assert!(state.active.is_none());
    assert!(state.entry.as_ref().unwrap().prospective_joint_evidence.is_some());
    assert_ne!(std::fs::read(root.join("state.json")).unwrap(),old);
    let bytes=std::fs::read(root.join("state.json")).unwrap();
    assert!(matches!(store.transact(generation,|_|Ok(())),
        Err(NativeStateError::Generation)));
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),bytes);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn r32d6c7_normal_link_child_crash_before_or_after_atomic_joint_fsync(){
    let Ok(target)=std::env::var("R32D6C7_CHILD_ROOT")else{return};
    use spotarb_native_market_core::{
        native_prospective_joint::audit_prospective_joint_chain_once,
        authoritative_state::NativeProspectiveJointWrite,
        private_rest::BinanceCredentials,
    };
    let phase=std::env::var("R32D6C7_CHILD_PHASE").unwrap();
    let (root,store,exec,generation)=r32d6c7_prospective_chain();
    let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    let report=audit_prospective_joint_chain_once(
        &exec,&store,generation,&okx,&bin,
        spotarb_native_market_core::private_https::wall_ms(),
        OkxDomain::Global,10_000,1,10).unwrap();
    let dest=std::path::PathBuf::from(target);
    std::fs::create_dir_all(&dest).unwrap();
    std::fs::write(dest.join("state.json"),
        std::fs::read(root.join("state.json")).unwrap()).unwrap();
    std::fs::File::open(dest.as_path()).unwrap().sync_all().unwrap();
    let restored=NativeStateStore::open(dest.join("state.json")).unwrap();
    if phase=="after"{
        let (saved,w)=restored.record_native_prospective_joint_window(report).unwrap();
        assert_eq!(w,NativeProspectiveJointWrite::Recorded);
        assert_eq!(saved.generation,generation+1);
        assert!(saved.active.is_none());
    }else{assert_eq!(phase,"before");}
    std::process::exit(81);
}
#[test]
fn r32d6c7_normal_link_after_restart_only_old_or_fully_sealed_recovery_state(){
    for phase in ["before","after"]{
        let path=root(&format!("r32d6c7-crash-{phase}"));
        std::fs::create_dir_all(&path).unwrap();
        let outcome=std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("r32d6c7_normal_link_child_crash_before_or_after_atomic_joint_fsync")
            .env("R32D6C7_CHILD_ROOT",&path)
            .env("R32D6C7_CHILD_PHASE",phase)
            .status().unwrap();
        assert_eq!(outcome.code(),Some(81));
        let reopened=NativeStateStore::open(path.join("state.json")).unwrap();
        let state=reopened.load_for_recovery().unwrap().unwrap();
        assert!(state.active.is_none());
        assert_eq!(state.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
        assert_eq!(state.entry.as_ref().unwrap().prospective_joint_evidence.is_some(),
            phase=="after");
        if phase=="after"{
            let chain=state.entry.as_ref().unwrap().prospective_joint_evidence.as_ref().unwrap();
            assert_eq!(chain.exposed_spot_unhedged_atoms_e18,600_000_000_000_000_000);
            assert!(!chain.may_send_order);
            assert!(!chain.may_promote_hedged);
            assert!(!chain.exchange_global_history_complete);
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}


/// R32D6C8B2B1 uses the independently compiled R32D6C7 all-read-only
/// two-exchange mock; no #[cfg(test)] synthetic Native Hedged state.
fn r32d6c8b2b1_joint_native_recovery()->(std::path::PathBuf,NativeStateStore,u64){
    use spotarb_native_market_core::{
        native_prospective_joint::audit_prospective_joint_chain_once,
        private_rest::BinanceCredentials,
    };
    let (root,store,exec,generation)=r32d6c7_prospective_chain();
    let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    let report=audit_prospective_joint_chain_once(
        &exec,&store,generation,&okx,&bin,
        spotarb_native_market_core::private_https::wall_ms(),
        OkxDomain::Global,10_000,1,10).unwrap();
    let (state,_) =store.record_native_prospective_joint_window(report).unwrap();
    assert_eq!(state.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
    assert!(state.active.is_none());
    (root,store,state.generation)
}
fn r32d6c8b2b1_config_path()->std::path::PathBuf{
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("config/native_trading.yaml")
}
#[test]
fn r32d6c8b2b1_normal_linked_joint_evidence_records_incomplete_real_budget_only(){
    use spotarb_native_market_core::native_real_risk_watch::RealWatchWrite;
    let (root,store,generation)=r32d6c8b2b1_joint_native_recovery();
    let old=std::fs::read(root.join("state.json")).unwrap();
    let now=spotarb_native_market_core::private_https::wall_ms();
    let (state,kind)=store.record_real_risk_diagnostic(
        generation,&r32d6c8b2b1_config_path(),now).unwrap();
    assert_eq!(kind,RealWatchWrite::Recorded);
    assert_eq!(state.generation,generation+1);
    assert_eq!(state.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
    assert!(state.active.is_none());
    assert_ne!(std::fs::read(root.join("state.json")).unwrap(),old);
    let entry=state.entry.as_ref().unwrap();
    let w=entry.real_risk_observation.as_ref().unwrap();
    assert_eq!(w.source_generation,generation);
    assert!(!w.entry_policy_bound); // historical C7 fixture (not a production admit)
    assert_eq!(w.observed_spot_fee_e12,100_000_000_000);
    assert_eq!(w.observed_futures_fee_e12,40_000_000_000);
    assert_eq!(w.known_entry_fee_e12,140_000_000_000);
    assert_eq!(w.outstanding_spot_exposure_e18,600_000_000_000_000_000);
    assert!(w.verified_real_closure_at_ms.is_none());
    assert!(w.verified_realized_net_pnl_e12.is_none());
    assert!(w.verified_day_loss_e12.is_none());
    assert!(w.verified_day_fees_e12.is_none());
    assert!(!w.verified_funding_complete);
    assert!(!w.independent_close_truth_complete);
    assert!(!w.may_open_real&&!w.may_send_order&&!w.may_promote_hedged);
    assert!(!w.cross_venue_atomicity_proven&&!w.exchange_global_history_complete);
    assert!(w.real_entry_denial().contains("Нельзя торговать"));
    assert!(store.reject_real_daily_slot(state.generation,
        &r32d6c8b2b1_config_path(),now).is_err());
    let bytes=std::fs::read(root.join("state.json")).unwrap();
    let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
    assert_eq!(reopened.load().unwrap().unwrap(),state);
    let (again,kind)=reopened.record_real_risk_diagnostic(
        state.generation,&r32d6c8b2b1_config_path(),now+1).unwrap();
    assert_eq!(kind,RealWatchWrite::Idempotent);
    assert_eq!(again,state);
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),bytes);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn astra_p106_r134_real_risk_source_generation_and_time_must_match_before_diagnostic(){
    use spotarb_native_market_core::{
        native_trading_config::NativeTradingConfig,
        native_real_risk_watch::{NativeRealRiskObservation,RealWatchError},
    };
    let (root,store,generation)=r32d6c8b2b1_joint_native_recovery();
    let cfg=NativeTradingConfig::load(&r32d6c8b2b1_config_path()).unwrap();
    let entry=store.load().unwrap().unwrap().entry.unwrap();
    let proof=entry.prospective_joint_evidence.as_ref().unwrap();
    assert_eq!(generation,proof.source_generation+1);
    let now=proof.captured_at_ms+1000;
    let good=NativeRealRiskObservation::from_bounded_entry(&entry,generation,now,&cfg).unwrap();
    assert_eq!(good.source_generation,generation);
    assert!(good.validate_against(&entry,generation+1).is_ok());
    // R134: stale/future generation or future-dated receipt cannot
    // create an apparently valid local risk source, even off-network.
    for source_gen in [generation+1,generation+2]{
       assert_eq!(NativeRealRiskObservation::from_bounded_entry(
            &entry,source_gen,now,&cfg).err(),Some(RealWatchError::Policy));
    }
    assert_eq!(NativeRealRiskObservation::from_bounded_entry(
        &entry,generation,proof.captured_at_ms-1,&cfg).err(),Some(RealWatchError::Policy));
    let mut forged=entry.clone();
    forged.prospective_joint_evidence.as_mut().unwrap().source_generation=
        generation.saturating_sub(2);
    assert_eq!(NativeRealRiskObservation::from_bounded_entry(
        &forged,generation,now,&cfg).err(),Some(RealWatchError::Policy));
    assert!(store.load().unwrap().unwrap().entry.unwrap().real_risk_observation.is_none());
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn r32d6c8b2b1_normal_linked_forged_closing_proof_and_daily_totals_refused_on_load(){
    let (root,store,generation)=r32d6c8b2b1_joint_native_recovery();
    let (state,_) =store.record_real_risk_diagnostic(
        generation,&r32d6c8b2b1_config_path(),
        spotarb_native_market_core::private_https::wall_ms()).unwrap();
    for case in 0..10{
        let mut forged=state.clone();
        let w=forged.entry.as_mut().unwrap().real_risk_observation.as_mut().unwrap();
        match case{
            0=>w.may_open_real=true,
            1=>w.may_send_order=true,
            2=>w.may_promote_hedged=true,
            3=>w.verified_real_closure_at_ms=Some(w.captured_at_ms),
            4=>w.verified_day_fees_e12=Some(0),
            5=>w.verified_day_loss_e12=Some(0),
            6=>w.verified_funding_complete=true,
            7=>w.outstanding_spot_exposure_e18=0,
            8=>w.spot_trade_ids.push("FAKE".into()),
            9=>w.config_sha256="f".repeat(63),
            _=>unreachable!(),
        }
        assert!(forged.validate().is_err(),"forged case {case}");
    }
    assert_eq!(store.load().unwrap().unwrap(),state);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn r32d6c8b2b1_normal_linked_no_joint_source_and_stale_source_write_nothing(){
    let (root,store,generation)=prepared();
    let now=spotarb_native_market_core::private_https::wall_ms();
    let old=std::fs::read(root.join("state.json")).unwrap();
    assert!(store.record_real_risk_diagnostic(
        generation,&r32d6c8b2b1_config_path(),now).is_err());
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),old);
    std::fs::remove_dir_all(root).unwrap();

    let (root,store,generation)=r32d6c8b2b1_joint_native_recovery();
    let src=store.load().unwrap().unwrap()
        .entry.unwrap().prospective_joint_evidence.unwrap();
    let stale=src.captured_at_ms+60_001;
    let old=std::fs::read(root.join("state.json")).unwrap();
    assert!(store.record_real_risk_diagnostic(
        generation,&r32d6c8b2b1_config_path(),stale).is_err());
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),old);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn r32d6c8b2b1_normal_linked_concurrent_cas_and_blocked_tmp_preserve_checkpoint(){
    use spotarb_native_market_core::native_real_risk_watch::RealWatchWrite;
    let (root,store,generation)=r32d6c8b2b1_joint_native_recovery();
    let now=spotarb_native_market_core::private_https::wall_ms();
    let old=std::fs::read(root.join("state.json")).unwrap();
    let blocker=root.join("state.tmp");
    std::fs::create_dir(&blocker).unwrap();
    assert!(store.record_real_risk_diagnostic(
        generation,&r32d6c8b2b1_config_path(),now).is_err());
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),old);
    std::fs::remove_dir(&blocker).unwrap();
    let s1=store.clone();let s2=store.clone();let c1=r32d6c8b2b1_config_path();
    let c2=c1.clone();
    let a=std::thread::spawn(move||s1.record_real_risk_diagnostic(generation,&c1,now));
    let b=std::thread::spawn(move||s2.record_real_risk_diagnostic(generation,&c2,now));
    let outcomes=[a.join().unwrap(),b.join().unwrap()];
    assert_eq!(outcomes.iter().filter(|x|matches!(
        x,Ok((_,RealWatchWrite::Recorded)))).count(),1);
    assert_eq!(outcomes.iter().filter(|x|matches!(
        x,Err(spotarb_native_market_core::authoritative_state::NativeStateError::Generation))).count(),1);
    let final_state=store.load().unwrap().unwrap();
    assert_eq!(final_state.generation,generation+1);
    assert!(final_state.entry.unwrap().real_risk_observation.is_some());
    std::fs::remove_dir_all(root).unwrap();
}


#[test]
fn r32d6c8b2b1_normal_linked_watcher_rejects_config_drift_without_state_change(){
    let (root,store,generation)=r32d6c8b2b1_joint_native_recovery();
    let conf=root.join("only-for-watch.yaml");
    let original_cfg=std::fs::read(r32d6c8b2b1_config_path()).unwrap();
    std::fs::write(&conf,&original_cfg).unwrap();
    let (state,_) =store.record_real_risk_diagnostic(
        generation,&conf,spotarb_native_market_core::private_https::wall_ms()).unwrap();
    let old=std::fs::read(root.join("state.json")).unwrap();
    let text=String::from_utf8(original_cfg).unwrap();
    let changed=text.replace("  heartbeat_interval_sec: 900\n","  heartbeat_interval_sec: 901\n");
    assert_ne!(changed,text);
    std::fs::write(&conf,changed).unwrap();
    assert!(store.record_real_risk_diagnostic(
        state.generation,&conf,spotarb_native_market_core::private_https::wall_ms()).is_err());
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),old);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn r32d6c8b2b1_child_exit_before_or_after_durable_unresolved_watch(){
    let Ok(target)=std::env::var("R32D6C8B2B_CHILD_ROOT")else{return};
    use spotarb_native_market_core::native_real_risk_watch::RealWatchWrite;
    let phase=std::env::var("R32D6C8B2B_CHILD_PHASE").unwrap();
    let (root,_store,generation)=r32d6c8b2b1_joint_native_recovery();
    let dir=std::path::PathBuf::from(target);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("state.json"),std::fs::read(root.join("state.json")).unwrap()).unwrap();
    std::fs::File::open(&dir).unwrap().sync_all().unwrap();
    let copy=NativeStateStore::open(dir.join("state.json")).unwrap();
    if phase=="after"{
        let (saved,w)=copy.record_real_risk_diagnostic(
            generation,&r32d6c8b2b1_config_path(),
            spotarb_native_market_core::private_https::wall_ms()).unwrap();
        assert_eq!(w,RealWatchWrite::Recorded);
        assert_eq!(saved.generation,generation+1);
    }else{assert_eq!(phase,"before")}
    std::process::exit(85);
}
#[test]
fn r32d6c8b2b1_exit_before_after_fsync_reopens_old_or_whole_unknown_watch(){
    for phase in ["before","after"]{
        let path=root(&format!("r32d6c8b2b1-crash-{phase}"));
        std::fs::create_dir_all(&path).unwrap();
        let status=std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("r32d6c8b2b1_child_exit_before_or_after_durable_unresolved_watch")
            .env("R32D6C8B2B_CHILD_ROOT",&path)
            .env("R32D6C8B2B_CHILD_PHASE",phase)
            .status().unwrap();
        assert_eq!(status.code(),Some(85));
        let store=NativeStateStore::open(path.join("state.json")).unwrap();
        let s=store.load_for_recovery().unwrap().unwrap();
        assert_eq!(s.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
        assert!(s.active.is_none());
        assert_eq!(s.entry.as_ref().unwrap().real_risk_observation.is_some(),phase=="after");
        if phase=="after"{
            let w=s.entry.as_ref().unwrap().real_risk_observation.as_ref().unwrap();
            assert!(w.verified_real_closure_at_ms.is_none());
            assert!(w.verified_day_loss_e12.is_none());
            assert!(w.verified_day_fees_e12.is_none());
            assert!(!w.may_send_order&&!w.may_open_real);
            assert_eq!(w.outstanding_spot_exposure_e18,600_000_000_000_000_000);
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}


// MICRO1 — an ambiguous *real-looking* Binance futures reply is not a
// confirmed trade. Quarantine the original durable intent in RecoveryRequired;
// do not create synthetic qty, fake fees, a Hedged position or a second send.
fn micro1_prepare(tag:&str)->(
    std::path::PathBuf,NativeStateStore,u64,
    spotarb_native_market_core::native_futures_uncertain_quarantine::UncertainFuturesResponse,
){
    let (dir,store,gen)=r32d6c4_proven_spot_bills_before_futures();
    let intent=format!("micro1-{tag}");
    let client=format!("micro1-{tag}-client");
    let (state,_)=store.prepare_futures_entry_hedge(gen,7,&intent,&client).unwrap();
    let report=spotarb_native_market_core::native_futures_uncertain_quarantine::UncertainFuturesResponse{
        intent_id:intent,client_id:client,
        exchange_order_id:Some("881211".to_owned()),
        reported_qty_e18:Some(200_000_000_000_000_000),
        reported_avg_price_e12:Some(decimal_to_atoms("101").unwrap()),
        terminal_reported:false,
        observed_at_ms:spotarb_native_market_core::private_https::wall_ms(),
    };
    (dir,store,state.generation,report)
}
#[test]
fn micro1_partial_binance_response_becomes_durable_unknown_without_creating_futures_fill(){
    use spotarb_native_market_core::native_futures_uncertain_quarantine::FuturesQuarantineWrite;
    let (dir,store,gen,r)=micro1_prepare("partial");
    let (after,status)=store.quarantine_uncertain_futures_execution(gen,&r).unwrap();
    assert_eq!(status,FuturesQuarantineWrite::Recorded);
    assert_eq!(after.generation,gen+1);
    assert!(after.active.is_none());
    let entry=after.entry.as_ref().unwrap();
    assert_eq!(entry.phase,NativeEntryPhase::RecoveryRequired);
    assert_eq!(entry.futures_filled_atoms_e18,0);
    assert_eq!(entry.spot_filled_atoms_e18,1_000_000_000_000_000_000);
    assert_eq!(entry.economics.entry.futures.filled_qty_atoms().unwrap(),0);
    let proof=entry.futures_uncertain_quarantine.as_ref().unwrap();
    assert_eq!(proof.reported_qty_e18(),Some(200_000_000_000_000_000));
    assert_eq!(proof.source_generation(),gen);
    assert!(!proof.overhedge_suspected());
    assert!(!proof.may_send_order()&&!proof.may_credit_fill());
    assert!(!proof.may_promote_hedged()&&!proof.confirmed_trades());
    let recovered=store.load_for_recovery().unwrap().unwrap();
    assert_eq!(recovered,after);
    let fresh=NativeStateStore::open(dir.join("state.json")).unwrap();
    assert_eq!(fresh.load().unwrap().unwrap(),after);
    let (idempotent,kind)=fresh.quarantine_uncertain_futures_execution(gen+1,&r).unwrap();
    assert_eq!(idempotent,after);
    assert_eq!(kind,FuturesQuarantineWrite::Idempotent);
    assert!(fresh.prepare_futures_entry_hedge(gen+1,7,"unsafe-second","unsafe-second-client").is_err());
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro1_unknown_or_overhedged_futures_response_stays_recovery_no_authority(){
    for (tag,qty,overshoot) in [("timeout",None,false),("overfill",Some(1_200_000_000_000_000_000u128),true)] {
        let (dir,store,gen,mut r)=micro1_prepare(tag);
        r.reported_qty_e18=qty;
        r.reported_avg_price_e12=if qty.is_none(){None}else{r.reported_avg_price_e12};
        let (state,_)=store.quarantine_uncertain_futures_execution(gen,&r).unwrap();
        let entry=state.entry.unwrap();
        let proof=entry.futures_uncertain_quarantine.unwrap();
        assert_eq!(proof.overhedge_suspected(),overshoot);
        assert_eq!(proof.reported_qty_e18(),qty);
        assert!(!proof.may_send_order()&&!proof.may_credit_fill());
        assert_eq!(entry.futures_filled_atoms_e18,0);
        assert_eq!(entry.phase,NativeEntryPhase::RecoveryRequired);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
#[test]
fn micro1_wrong_intent_stale_generation_and_malformed_response_never_change_state(){
    let (dir,store,gen,r)=micro1_prepare("denials");
    let original=std::fs::read(dir.join("state.json")).unwrap();
    for what in ["wrong-intent","wrong-client","wrong-order","zero-qty",
                 "zero-price","old-clock","duplicate-prepared","missing-base"]{
        let mut attempt=r.clone();
        match what{
            "wrong-intent"=>attempt.intent_id="foreign-intent".into(),
            "wrong-client"=>attempt.client_id="foreign-client".into(),
            "wrong-order"=>attempt.exchange_order_id=Some("BAD-ORDER".into()),
            "zero-qty"=>attempt.reported_qty_e18=Some(0),
            "zero-price"=>attempt.reported_avg_price_e12=Some(0),
            "old-clock"=>attempt.observed_at_ms=0,
            "duplicate-prepared"=>attempt.intent_id=r.client_id.clone(),
            "missing-base"=>attempt.client_id="".into(),
            _=>unreachable!(),
        }
        assert!(store.quarantine_uncertain_futures_execution(gen,&attempt).is_err(),
            "{what} entered futures quarantine");
        assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
    }
    assert!(store.quarantine_uncertain_futures_execution(gen-1,&r).is_err());
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro1_owner_guards_refuse_erasure_ledger_forgery_and_trade_authority(){
    let (dir,store,gen,r)=micro1_prepare("tamper");
    let (state,_)=store.quarantine_uncertain_futures_execution(gen,&r).unwrap();
    let before=std::fs::read(dir.join("state.json")).unwrap();
    for kind in ["delete-entry","erase","swap-intent","spoof-sent",
                 "spoof-credit","spoof-hedged","spoof-fees",
                 "forge-confirmed-qty","alter-reported-qty"]{
        let mut fake=state.clone();
        let entry=fake.entry.as_mut().unwrap();
        match kind{
            "delete-entry"=>fake.entry=None,
            "erase"=>entry.futures_uncertain_quarantine=None,
            "swap-intent"=>entry.intents.last_mut().unwrap().client_order_id=Some("foreign".into()),
            "forge-confirmed-qty"=>entry.futures_filled_atoms_e18=200_000_000_000_000_000,
            _=>{
                let raw=serde_json::to_string(&entry.futures_uncertain_quarantine).unwrap();
                let (old,new)=match kind{
                    "spoof-sent"=>("\"may_send_order\":false","\"may_send_order\":true"),
                    "spoof-credit"=>("\"may_credit_fill\":false","\"may_credit_fill\":true"),
                    "spoof-hedged"=>("\"may_promote_hedged\":false","\"may_promote_hedged\":true"),
                    "spoof-fees"=>("\"futures_fees_confirmed\":false","\"futures_fees_confirmed\":true"),
                    "alter-reported-qty"=>("\"reported_qty_e18\":200000000000000000","\"reported_qty_e18\":100000000000000000"),
                    _=>unreachable!(),
                };
                let patched=raw.replacen(old,new,1);
                assert_ne!(patched,raw,"ineffective {kind} mutation");
                entry.futures_uncertain_quarantine=serde_json::from_str(&patched).unwrap();
            }
        }
        fake.generation=state.generation+1;
        assert!(store.replace(state.generation,&fake).is_err(), "tampering {kind} succeeded");
        assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
    }
    assert!(store.transact(state.generation,|s|{
        s.entry.as_mut().unwrap().futures_uncertain_quarantine=None;Ok(())
    }).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro1_competing_cas_and_file_failure_recovery_is_idempotent(){
    use spotarb_native_market_core::native_futures_uncertain_quarantine::FuturesQuarantineWrite;
    let (dir,store,gen,r)=micro1_prepare("race");
    std::thread::scope(|scope|{
        let a=scope.spawn(||store.quarantine_uncertain_futures_execution(gen,&r));
        let b=scope.spawn(||store.quarantine_uncertain_futures_execution(gen,&r));
        let results=[a.join().unwrap(),b.join().unwrap()];
        assert_eq!(results.iter().filter(|x|matches!(x,Ok((_,FuturesQuarantineWrite::Recorded)))).count(),1);
        assert_eq!(results.iter().filter(|x|x.is_err()).count()
            +results.iter().filter(|x|matches!(x,Ok((_,FuturesQuarantineWrite::Idempotent)))).count(),1);
    });
    assert_eq!(store.load().unwrap().unwrap().generation,gen+1);
    let mut conflict=r.clone();conflict.reported_qty_e18=Some(300_000_000_000_000_000);
    let before=std::fs::read(dir.join("state.json")).unwrap();
    assert!(store.quarantine_uncertain_futures_execution(gen+1,&conflict).is_err());
    assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
    std::fs::remove_dir_all(dir).unwrap();

    let (dir,store,gen,r)=micro1_prepare("disk");
    let snapshot=std::fs::read(dir.join("state.json")).unwrap();
    let blocker=dir.join("state.tmp");
    std::fs::create_dir(&blocker).unwrap();
    assert!(store.quarantine_uncertain_futures_execution(gen,&r).is_err());
    assert_eq!(snapshot,std::fs::read(dir.join("state.json")).unwrap());
    std::fs::remove_dir(&blocker).unwrap();
    let (saved,result)=store.quarantine_uncertain_futures_execution(gen,&r).unwrap();
    assert_eq!(result,FuturesQuarantineWrite::Recorded);
    assert_eq!(saved.generation,gen+1);
    assert!(saved.active.is_none());
    std::fs::remove_dir_all(dir).unwrap();
}


// MICRO2: normal linked RUST crate signed GET-only reconciliation of durable
// MICRO1 futures UNKNOWN. Reuses existing C2+C3 independent spot proof and
// C7's Binance order/account/fills fixture; never submits any mutation.
fn micro2_seed(tag:&str)->(
 std::path::PathBuf,NativeStateStore,C7LinkedExchange,u64,
){
 use spotarb_native_market_core::{
    authoritative_state::{NativeSpotFirstLegWrite,NativeSpotBillsWrite},
    private_rest::OrderObservation,
 };
 let (dir,store,gen)=prepared();
 let orig=store.load().unwrap().unwrap().entry.unwrap().plan.created_at_ms;
 let client=format!("micro2-{tag}-client");
 let exec=C7LinkedExchange::new(orig,&client);
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let now=spotarb_native_market_core::private_https::wall_ms();
 let proof=audit_okx_first_leg_before_hedge(
    &exec,&store,gen,&okx,now,OkxDomain::Global,1).unwrap();
 let (one,status)=store.record_native_spot_first_leg_evidence(proof).unwrap();
 assert_eq!(status,NativeSpotFirstLegWrite::Recorded);
 let bills=audit_okx_first_leg_cash_bills_window(
    &exec,&store,one.generation,&okx,now,OkxDomain::Global,1).unwrap();
 let (two,status)=store.record_native_spot_prehedge_bills_evidence(bills).unwrap();
 assert_eq!(status,NativeSpotBillsWrite::Recorded);
 let (futures,_permit)=store.prepare_futures_entry_hedge(
    two.generation,7,&format!("micro2-{tag}-futures"),&client).unwrap();
 let obs=OrderObservation{
    class:spotarb_native_market_core::private_rest::RestOutcomeClass::Acknowledged,
    terminal:false,filled:Some("0.4".into()),status:Some("PARTIALLY_FILLED".into()),
    order_id:Some("124".into()),client_order_id:Some(client),
    reason:"MICRO2_MOCK_PLACEMENT_ACK_NEVER_TRADE_PROOF",
 };
 let (recorded,_)=store.quarantine_futures_dispatch_observation(
    futures.generation,&obs,now).unwrap();
 assert!(recorded.active.is_none());
 assert_eq!(recorded.entry.as_ref().unwrap().futures_filled_atoms_e18,0);
 (dir,store,exec,recorded.generation)
}
#[test]
fn micro2_quarantine_reconciles_signed_binance_order_account_and_two_trades_no_send(){
 use spotarb_native_market_core::{
     native_futures_uncertain_quarantine::probe_quarantined_futures_entry_readonly_once,
     native_entry_recovery::{EntryProbeVerdict,EntryFillVerdict},
     private_rest::BinanceCredentials,
     authoritative_state::NativeStateStore,
 };
 let (dir,store,exec,gen)=micro2_seed("positive");
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let prev_calls=exec.total();
 let proof=probe_quarantined_futures_entry_readonly_once(
    &exec,&store,gen,&okx,&bin,
    spotarb_native_market_core::private_https::wall_ms(),
    OkxDomain::Global,10_000).unwrap();
 assert_eq!(proof.order_account.verdict,EntryProbeVerdict::TerminalCandidateNeedsFillFeesAndInventoryTruth);
 let trade=proof.detailed_trades.as_ref().unwrap();
 assert_eq!(trade.verdict,EntryFillVerdict::PriorSpotFeeOriginUnverified);
 assert_eq!(trade.fill_count,2);
 assert_eq!(trade.filled_atoms_e18,400_000_000_000_000_000);
 assert_eq!(exec.total()-prev_calls,11,"one signed order GET, nine account GET and one trade GET");
 assert!(!proof.may_send_order&&!proof.may_promote_hedged);
 assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap(),
   "GET reconciliation cannot change durable futures exposure or exit UNKNOWN");
 let reopened=NativeStateStore::open(dir.join("state.json")).unwrap();
 let latest=reopened.load_for_recovery().unwrap().unwrap();
 assert_eq!(latest.generation,gen);
 assert_eq!(latest.entry.unwrap().phase,NativeEntryPhase::RecoveryRequired);
 assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro2_stale_generation_foreign_quarantine_and_missing_source_do_zero_gets(){
 use spotarb_native_market_core::{
     native_futures_uncertain_quarantine::probe_quarantined_futures_entry_readonly_once,
     private_rest::BinanceCredentials,
 };
 let (dir,store,exec,gen)=micro2_seed("stale");
 let previous=std::fs::read(dir.join("state.json")).unwrap();
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let count=exec.total();
 assert!(probe_quarantined_futures_entry_readonly_once(
   &exec,&store,gen-1,&okx,&bin,
   spotarb_native_market_core::private_https::wall_ms(),
   OkxDomain::Global,10_000).is_err());
 assert_eq!(count,exec.total());
 assert_eq!(previous,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,gen)=r32d6c4_proven_spot_bills_before_futures();
 let (_,_) =store.prepare_futures_entry_hedge(
    gen,7,"no-quarantine","no-quarantine-client").unwrap();
 let old=store.load().unwrap().unwrap().generation;
 let client=C7LinkedExchange::new(
   spotarb_native_market_core::private_https::wall_ms().saturating_sub(10_000),
   "no-quarantine-client");
 let count=client.total();
 assert!(probe_quarantined_futures_entry_readonly_once(
   &client,&store,old,&okx,&bin,
   spotarb_native_market_core::private_https::wall_ms(),
   OkxDomain::Global,10_000).is_err());
 assert_eq!(client.total(),count);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro2_bad_balance_suppresses_trade_probe_and_keeps_uncertain_quarantine(){
 use spotarb_native_market_core::{
    native_futures_uncertain_quarantine::probe_quarantined_futures_entry_readonly_once,
    native_entry_recovery::EntryProbeVerdict,
    private_rest::BinanceCredentials,
 };
 let (dir,store,exec,gen)=micro2_seed("bad-balance");
 exec.fault(6);
 let old=std::fs::read(dir.join("state.json")).unwrap();
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let n=exec.total();
 let read=probe_quarantined_futures_entry_readonly_once(
   &exec,&store,gen,&okx,&bin,
   spotarb_native_market_core::private_https::wall_ms(),
   OkxDomain::Global,10_000).unwrap();
 assert_eq!(read.order_account.verdict,EntryProbeVerdict::AccountConflict);
 assert!(read.detailed_trades.is_none());
 assert_eq!(exec.total()-n,10);
 assert!(!read.may_send_order);
 assert_eq!(old,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro2_foreign_or_bad_order_ack_cannot_replace_original_client_or_credit_fills(){
 use spotarb_native_market_core::private_rest::{OrderObservation,RestOutcomeClass};
 let (dir,store,gen)=r32d6c4_proven_spot_bills_before_futures();
 let (futures,_)=store.prepare_futures_entry_hedge(
    gen,7,"micro2-malicious","micro2-malicious-client").unwrap();
 let fake=OrderObservation{
    class:RestOutcomeClass::Acknowledged,terminal:true,
    filled:Some("0.9".into()),status:Some("FILLED".into()),
    order_id:Some("1999".into()),client_order_id:Some("foreign-id".into()),
    reason:"UNTRUSTED_FOREIGN_RESPONSE",
 };
 let (after,_)=store.quarantine_futures_dispatch_observation(
    futures.generation,&fake,spotarb_native_market_core::private_https::wall_ms()).unwrap();
 let proof=after.entry.unwrap();
 assert_eq!(proof.phase,NativeEntryPhase::RecoveryRequired);
 assert_eq!(proof.futures_filled_atoms_e18,0);
 let witness=proof.futures_uncertain_quarantine.as_ref().unwrap();
 assert_eq!(witness.reported_qty_e18(),None);
 assert_eq!(witness.reported_order_id(),None);
 assert_eq!(witness.recorded_client_id(),"micro2-malicious-client");
 assert!(!witness.may_send_order()&&!witness.may_credit_fill());
 std::fs::remove_dir_all(dir).unwrap();
}


// MICRO3: durable exact MICRO1/MICRO2 -> original R32D3 per-trade checkpoint,
// still no native futures exposure credit and no auto order or recovery permit.
fn micro3_probe(
    store:&NativeStateStore, exec:&C7LinkedExchange, gen:u64
)->spotarb_native_market_core::native_futures_uncertain_quarantine::FuturesQuarantineGetReport{
 use spotarb_native_market_core::{
    native_futures_uncertain_quarantine::probe_quarantined_futures_entry_readonly_once,
    private_rest::BinanceCredentials,
 };
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 probe_quarantined_futures_entry_readonly_once(
    exec,store,gen,&okx,&bin,spotarb_native_market_core::private_https::wall_ms(),
    OkxDomain::Global,10_000).unwrap()
}
#[test]
fn micro3_durable_exact_trades_fees_partial_hedge_advice_and_restart(){
 use spotarb_native_market_core::{
    native_futures_uncertain_quarantine::FuturesQuarantineRecoveryAdvice,
    authoritative_state::{NativeEntryRecoveryWrite,NativeEntryRecoveryQuality},
 };
 let (dir,store,exec,gen)=micro2_seed("micro3-positive");
 let scanned=micro3_probe(&store,&exec,gen);
 let original=std::fs::read(dir.join("state.json")).unwrap();
 let (state,kind,advice)=store.record_quarantined_futures_get_checkpoint(scanned).unwrap();
 assert_eq!(kind,NativeEntryRecoveryWrite::Recorded);
 assert_eq!(state.generation,gen+1);
 assert_ne!(std::fs::read(dir.join("state.json")).unwrap(),original);
 assert_eq!(advice,FuturesQuarantineRecoveryAdvice::PartialHedgeManualDecision{
     verified_short_e18:400_000_000_000_000_000,
     unmatched_spot_e18:600_000_000_000_000_000,
 });
 let entry=state.entry.as_ref().unwrap();
 assert_eq!(entry.phase,NativeEntryPhase::RecoveryRequired);
 assert!(state.active.is_none());
 assert_eq!(entry.futures_filled_atoms_e18,0);
 assert_eq!(entry.economics.entry.futures.filled_qty_atoms().unwrap(),0);
 let cp=entry.recovery_checkpoint.as_ref().unwrap();
 assert_eq!(cp.quality,NativeEntryRecoveryQuality::FuturesPriorSpotUnverified);
 assert_eq!(cp.order_id,"124");
 assert_eq!(cp.ledger.fills.len(),2);
 assert_eq!(cp.ledger.filled_qty_atoms(),decimal_to_atoms("0.4").unwrap());
 assert_eq!(cp.ledger.fee_cost_quote_atoms,decimal_to_atoms("0.04").unwrap() as i128);
 assert!(cp.ledger.fee_evidence_complete);
 assert_eq!(cp.ledger.fills.keys().cloned().collect::<Vec<_>>(),vec!["1001","1002"]);
 let restarted=NativeStateStore::open(dir.join("state.json")).unwrap();
 assert_eq!(restarted.load_for_recovery().unwrap().unwrap(),state);
 assert_eq!(restarted.load().unwrap().unwrap()
    .quarantined_futures_recovery_advice().unwrap(),advice);
 assert!(restarted.prepare_futures_entry_hedge(state.generation,7,
    "blind-second","blind-second-client").is_err());
 assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro3_conflicting_placement_order_hint_rejected_despite_consistent_get(){
 let (dir,store,gen)=r32d6c4_proven_spot_bills_before_futures();
 let client="micro3-wrong-hint-client";
 let (prep,_)=store.prepare_futures_entry_hedge(gen,7,"micro3-wrong-hint",client).unwrap();
 let obs=spotarb_native_market_core::private_rest::OrderObservation{
     class:spotarb_native_market_core::private_rest::RestOutcomeClass::Unknown,
     terminal:false,filled:Some("0.4".into()),status:None,
     order_id:Some("999".into()),client_order_id:Some(client.into()),
     reason:"UNTRUSTED_WRONG_ORDER_HINT",
 };
 let (state,_)=store.quarantine_futures_dispatch_observation(
    prep.generation,&obs,spotarb_native_market_core::private_https::wall_ms()).unwrap();
 let execution=C7LinkedExchange::new(
    state.entry.as_ref().unwrap().plan.created_at_ms,client);
 let scanned=micro3_probe(&store,&execution,state.generation);
 let before=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.record_quarantined_futures_get_checkpoint(scanned).is_err());
 assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
 assert_eq!(store.load().unwrap().unwrap().entry.unwrap().recovery_checkpoint,None);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro3_no_signed_get_cannot_mint_trades_or_manual_hedge_permission(){
 use spotarb_native_market_core::native_futures_uncertain_quarantine::{
     FuturesQuarantineRecoveryAdvice,probe_quarantined_futures_entry_readonly_once,
 };
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let (dir,store,exec,gen)=micro2_seed("micro3-no-get");
 assert_eq!(store.load().unwrap().unwrap().quarantined_futures_recovery_advice().unwrap(),
    FuturesQuarantineRecoveryAdvice::ManualReviewRequired);
 let original=std::fs::read(dir.join("state.json")).unwrap();
 exec.fault(6);
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let scan=probe_quarantined_futures_entry_readonly_once(
    &exec,&store,gen,&okx,&bin,spotarb_native_market_core::private_https::wall_ms(),
    OkxDomain::Global,10_000).unwrap();
 assert!(scan.detailed_trades.is_none());
 assert!(store.record_quarantined_futures_get_checkpoint(scan).is_err());
 assert_eq!(original,std::fs::read(dir.join("state.json")).unwrap());
 assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro3_checkpoint_immutable_under_cas_tamper_and_recomputed_advice(){
 let (dir,store,exec,gen)=micro2_seed("micro3-immutable");
 let scanned=micro3_probe(&store,&exec,gen);
 let (state,_,_)=store.record_quarantined_futures_get_checkpoint(scanned).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 for what in ["erase","change-qty","swap-trade","change-fee","change-client",
              "remove-quarantine","delete-entry","credit-unconfirmed-futures"]{
    let mut edited=state.clone();
    let entry=edited.entry.as_mut().unwrap();
    match what{
       "erase"=>entry.recovery_checkpoint=None,
       "remove-quarantine"=>entry.futures_uncertain_quarantine=None,
       "delete-entry"=>edited.entry=None,
       "credit-unconfirmed-futures"=>entry.futures_filled_atoms_e18=400_000_000_000_000_000,
       _=>{
           let raw=serde_json::to_string(&entry.recovery_checkpoint).unwrap();
           let (a,b)=match what{
               "change-qty"=>("\"requested_qty_atoms\":1000000000000",
                              "\"requested_qty_atoms\":900000000000"),
               "swap-trade"=>("\"1001\"","\"9999\""),
               "change-fee"=>("\"fee_cost_quote_atoms\":40000000000",
                              "\"fee_cost_quote_atoms\":1"),
               "change-client"=>("\"client_id\":\"","\"client_id\":\"foreign-"),
               _=>unreachable!(),
           };
           let changed=raw.replacen(a,b,1);
           assert_ne!(raw,changed,"{what} failed to mutate witness");
           entry.recovery_checkpoint=serde_json::from_str(&changed).unwrap();
       }
    }
    edited.generation=state.generation+1;
    assert!(store.replace(state.generation,&edited).is_err(),"forged {what} accepted");
    assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
 }
 assert!(store.transact(state.generation,|s|{
    s.entry.as_mut().unwrap().recovery_checkpoint=None;Ok(())
 }).is_err());
 assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro3_checkpoint_duplicate_signed_get_is_idempotent_and_stale_fails(){
 use spotarb_native_market_core::authoritative_state::NativeEntryRecoveryWrite;
 let (dir,store,exec,gen)=micro2_seed("micro3-idempotent");
 let scanned=micro3_probe(&store,&exec,gen);
 let (saved,kind,advice)=store.record_quarantined_futures_get_checkpoint(scanned).unwrap();
 assert_eq!(kind,NativeEntryRecoveryWrite::Recorded);
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let restarted=NativeStateStore::open(dir.join("state.json")).unwrap();
 let newscan=micro3_probe(&restarted,&exec,saved.generation);
 let (replay,kind2,advice2)=restarted.record_quarantined_futures_get_checkpoint(newscan).unwrap();
 assert_eq!(kind2,NativeEntryRecoveryWrite::Idempotent);
 assert_eq!(replay,saved); assert_eq!(advice2,advice);
 assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
 assert!(restarted.record_quarantined_futures_get_checkpoint(
     micro3_probe(&restarted,&exec,saved.generation-0))
     .is_ok(),"exact same replay remains harmless");
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro3_synthetic_disk_failure_does_not_partial_write_and_retry_succeeds(){
 let (dir,store,exec,gen)=micro2_seed("micro3-disk");
 let checkpoint=micro3_probe(&store,&exec,gen);
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let blocker=dir.join("state.tmp");
 std::fs::create_dir(&blocker).unwrap();
 assert!(store.record_quarantined_futures_get_checkpoint(checkpoint).is_err());
 assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir(&blocker).unwrap();
 let source=micro3_probe(&store,&exec,gen);
 let (state,_,advice)=store.record_quarantined_futures_get_checkpoint(source).unwrap();
 assert_eq!(state.generation,gen+1);
 assert!(matches!(advice,spotarb_native_market_core::native_futures_uncertain_quarantine::
    FuturesQuarantineRecoveryAdvice::PartialHedgeManualDecision{..}));
 assert!(state.active.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}


// MICRO4: independently repeat the original Binance C5 income/position/
// symbol-wide GET around a frozen MICRO3 checkpoint, then an OKX+Binance
// C6 cross-venue bracket. Recommendations remain NO-AUTO-ORDER.
fn micro4_seed(tag:&str)->(
 std::path::PathBuf,NativeStateStore,C7LinkedExchange,u64
){
 let (dir,store,exec,gen)=micro2_seed(tag);
 let get=micro3_probe(&store,&exec,gen);
 let (saved,_,_)=store.record_quarantined_futures_get_checkpoint(get).unwrap();
 (dir,store,exec,saved.generation)
}
#[test]
fn micro4_full_27_plus_29_signed_mock_gets_persist_original_pair_without_action(){
 use spotarb_native_market_core::{
   native_futures_uncertain_quarantine::QuarantinedOperatorReview,
   private_rest::BinanceCredentials,
   authoritative_state::NativeStateStore,
 };
 let (dir,store,exec,gen)=micro4_seed("complete");
 let original=std::fs::read(dir.join("state.json")).unwrap();
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let prior=exec.total();
 let (saved,summary)=store.audit_and_record_quarantined_full_window_once(
    &exec,gen,&okx,&bin,spotarb_native_market_core::private_https::wall_ms(),
    OkxDomain::Global,10_000,1,10).unwrap();
 assert_eq!(exec.total()-prior,56,"C5=27 Binance GET + C6=29 paired GET");
 assert_eq!(saved.generation,gen+2);
 assert_eq!(summary.generation,gen+2);
 assert_eq!(summary.binance_account_brackets,2);
 assert_eq!(summary.cross_venue_account_brackets,2);
 assert_eq!(summary.review,QuarantinedOperatorReview::ManualRecoveryRequired{
    observed_short_e18:400_000_000_000_000_000,
    unmatched_spot_e18:600_000_000_000_000_000,
    confirmed_trade_count:2,
    confirmed_fee_e12:decimal_to_atoms("0.04").unwrap() as i128,
 });
 assert!(!summary.may_send_order&&!summary.may_auto_hedge
     &&!summary.may_auto_unwind&&!summary.may_promote_hedged
     &&!summary.may_verify_closed&&!summary.globally_complete
     &&!summary.cross_venue_atomicity_proven);
 let e=saved.entry.as_ref().unwrap();
 assert!(saved.active.is_none());
 assert_eq!(e.phase,NativeEntryPhase::RecoveryRequired);
 assert_eq!(e.futures_filled_atoms_e18,0);
 assert_eq!(e.economics.entry.futures.filled_qty_atoms().unwrap(),0);
 assert_eq!(e.futures_entry_window_evidence.as_ref().unwrap().trade_ids,
     vec!["1001","1002"]);
 assert_eq!(e.prospective_joint_evidence.as_ref().unwrap()
     .exposed_spot_unhedged_atoms_e18,600_000_000_000_000_000);
 assert_ne!(std::fs::read(dir.join("state.json")).unwrap(),original);
 let reopened=NativeStateStore::open(dir.join("state.json")).unwrap();
 assert_eq!(reopened.load_for_recovery().unwrap().unwrap(),saved);
 assert_eq!(reopened.load().unwrap().unwrap()
    .quarantined_supervised_assessment().unwrap(),summary);
 assert!(reopened.prepare_futures_entry_hedge(saved.generation,7,
      "forbidden-rehedge","forbidden-client").is_err());
 assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro4_incomplete_income_and_foreign_trade_fail_without_any_c5_commit(){
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 for (tag,fault) in [("unexpected-funding",4),("foreign-symbol-trades",3),
     ("missing-trade",5)]{
    let (dir,store,exec,gen)=micro4_seed(tag);
    let original=std::fs::read(dir.join("state.json")).unwrap();
    let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    exec.fault(fault);
    assert!(store.audit_and_record_quarantined_binance_window_once(
         &exec,gen,&bin,spotarb_native_market_core::private_https::wall_ms(),
         10_000,1,10).is_err(),"source fault {tag} wrongly accepted");
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
    let e=store.load().unwrap().unwrap().entry.unwrap();
    assert!(e.futures_entry_window_evidence.is_none());
    assert!(e.prospective_joint_evidence.is_none());
    assert_eq!(e.futures_filled_atoms_e18,0);
    std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn micro4_failed_second_account_bracket_keeps_first_durable_and_manual_resume_only(){
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let (dir,store,exec,gen)=micro4_seed("second-fails");
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let now=spotarb_native_market_core::private_https::wall_ms();
 let first=store.audit_and_record_quarantined_binance_window_once(
     &exec,gen,&bin,now,10_000,1,10).unwrap();
 assert_eq!(first.generation,gen+1);
 let first_bytes=std::fs::read(dir.join("state.json")).unwrap();
 exec.fault(3);
 assert!(store.audit_and_record_quarantined_joint_window_once(
    &exec,first.generation,&okx,&bin,now,OkxDomain::Global,
    10_000,1,10).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),first_bytes);
 let e=store.load().unwrap().unwrap().entry.unwrap();
 assert!(e.futures_entry_window_evidence.is_some());
 assert!(e.prospective_joint_evidence.is_none());
 assert!(store.load().unwrap().unwrap().quarantined_supervised_assessment().is_err());
 let calls=exec.total();
 assert!(store.audit_and_record_quarantined_binance_window_once(
    &exec,first.generation,&bin,now,10_000,1,10).is_err());
 assert_eq!(exec.total(),calls,"cannot implicitly redo first C5 history");
 exec.fault(0);
 let (saved,summary)=store.audit_and_record_quarantined_joint_window_once(
    &exec,first.generation,&okx,&bin,now,OkxDomain::Global,
    10_000,1,10).unwrap();
 assert_eq!(saved.generation,gen+2);
 assert_eq!(summary.generation,gen+2);
 assert!(matches!(summary.review,
    spotarb_native_market_core::native_futures_uncertain_quarantine::
       QuarantinedOperatorReview::ManualRecoveryRequired{..}));
 assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro4_stale_generation_or_missing_checkpoint_declines_before_any_private_get(){
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let (dir,store,exec,gen)=micro4_seed("stale");
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let hits=exec.total();
 assert!(store.audit_and_record_quarantined_binance_window_once(
   &exec,gen-1,&bin,spotarb_native_market_core::private_https::wall_ms(),
   10_000,1,10).is_err());
 assert_eq!(hits,exec.total());
 assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,exec,gen)=micro2_seed("no-micro3");
 let hits=exec.total();
 assert!(store.audit_and_record_quarantined_binance_window_once(
   &exec,gen,&bin,spotarb_native_market_core::private_https::wall_ms(),
   10_000,1,10).is_err());
 assert_eq!(hits,exec.total());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro4_c5_and_joint_c6_cas_immutability_even_when_caller_tries_delete_entire_entry(){
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let (dir,store,exec,gen)=micro4_seed("tamper");
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let (saved,_)=store.audit_and_record_quarantined_full_window_once(
   &exec,gen,&okx,&bin,spotarb_native_market_core::private_https::wall_ms(),
   OkxDomain::Global,10_000,1,10).unwrap();
 let original=std::fs::read(dir.join("state.json")).unwrap();
 for case in ["remove-c5","remove-c6","delete-entry","fake-future-fees",
              "fake-futures-short","fake-spot-net","fake-send","fake-hedged"]{
    let mut changed=saved.clone();
    let e=changed.entry.as_mut().unwrap();
    match case{
        "remove-c5"=>e.futures_entry_window_evidence=None,
        "remove-c6"=>e.prospective_joint_evidence=None,
        "delete-entry"=>changed.entry=None,
        "fake-future-fees"=>e.futures_entry_window_evidence.as_mut().unwrap()
            .signed_quote_fee_cost_atoms_e12+=1,
        "fake-futures-short"=>e.futures_entry_window_evidence.as_mut().unwrap()
            .observed_short_atoms_e18+=1,
        "fake-spot-net"=>e.prospective_joint_evidence.as_mut().unwrap()
            .spot_available_net_atoms_e18+=1,
        "fake-send"=>e.prospective_joint_evidence.as_mut().unwrap().may_send_order=true,
        "fake-hedged"=>e.futures_entry_window_evidence.as_mut().unwrap().may_promote_hedged=true,
        _=>unreachable!(),
    }
    changed.generation=saved.generation+1;
    assert!(store.replace(saved.generation,&changed).is_err(),
        "source forgery {case} accepted");
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
 }
 assert!(store.transact(saved.generation,|s|{
     s.entry.as_mut().unwrap().futures_entry_window_evidence=None;Ok(())
 }).is_err());
 assert!(store.transact(saved.generation,|s|{
     s.entry.as_mut().unwrap().prospective_joint_evidence=None;Ok(())
 }).is_err());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro4_failed_tempfile_fsync_c5_write_preserves_original_micro3_checkpoint(){
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let (dir,store,exec,gen)=micro4_seed("disk");
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let original=std::fs::read(dir.join("state.json")).unwrap();
 let temp=dir.join("state.tmp");
 std::fs::create_dir(&temp).unwrap();
 assert!(store.audit_and_record_quarantined_binance_window_once(
    &exec,gen,&bin,spotarb_native_market_core::private_https::wall_ms(),
    10_000,1,10).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
 std::fs::remove_dir(&temp).unwrap();
 let ok=store.audit_and_record_quarantined_binance_window_once(
    &exec,gen,&bin,spotarb_native_market_core::private_https::wall_ms(),
    10_000,1,10).unwrap();
 assert_eq!(ok.generation,gen+1);
 assert!(ok.entry.as_ref().unwrap().prospective_joint_evidence.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn micro4_binance_wallet_changes_between_account_brackets_rejected_before_c5_write(){
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 // A different wallet balance known BEFORE the first C5 snapshot is not
 // detectable by comparing TWO later snapshots. Here the cash truly MOVES
 // between the independently signed checks; the C5 audit must deny.
 struct MovingWallet {
     inner:C7LinkedExchange,
     observed_wallet_reads:AtomicUsize,
 }
 impl HttpExecutor for MovingWallet {
     fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
         let wallet=url::Url::parse(&req.url).map(|u|u.path()=="/fapi/v3/balance")
             .unwrap_or(false);
         if wallet&&self.observed_wallet_reads.fetch_add(1,Ordering::SeqCst)>0{
             self.inner.fault(2); // 100 USDT -> 99.9 USDT
         }
         self.inner.execute_request(req)
     }
 }
 let (dir,store,exec,gen)=micro4_seed("moving-wallet");
 let http=MovingWallet{inner:exec,observed_wallet_reads:AtomicUsize::new(0)};
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.audit_and_record_quarantined_binance_window_once(
   &http,gen,&bin,spotarb_native_market_core::private_https::wall_ms(),
   10_000,1,10).is_err());
 assert!(http.observed_wallet_reads.load(Ordering::SeqCst)>=2);
 assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
 assert!(store.load().unwrap().unwrap().entry.unwrap()
    .futures_entry_window_evidence.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}


// MICRO5B1: initial independently signed *before first spot* Binance wallet.
// Later C5/C6 mock shows 100 USDT and 0.04 USDT two execution commissions.
struct Micro5PreEntryGet {wallet:String,updated:u64}
impl HttpExecutor for Micro5PreEntryGet{
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
  assert_eq!(req.method,HttpMethod::Get);
  assert!(!req.mutation);
  let url=url::Url::parse(&req.url).unwrap();
  let body=match url.path(){
   "/fapi/v1/positionSide/dual"=>r#"{"dualSidePosition":false}"#.into(),
   "/fapi/v1/accountConfig"=>r#"{"multiAssetsMargin":false}"#.into(),
   "/fapi/v1/openOrders"=>"[]".into(),
   "/fapi/v3/positionRisk"=>r#"[{"symbol":"BNTUSDT","positionAmt":"0","positionSide":"BOTH"}]"#.into(),
   "/fapi/v3/balance"=>format!(
     r#"[{{"asset":"USDT","balance":"{}","availableBalance":"{}","updateTime":{}}}]"#,
     self.wallet,self.wallet,self.updated),
   p=>panic!("unexpected PRE-entry GET {p}"),
  };
  Ok(HttpResponse{status:200,headers:vec![],body})
 }
}
fn micro5_prepared_with_wallet(start_usdt:&str)->(std::path::PathBuf,NativeStateStore,u64){
    let path=root("micro5-wallet-first-leg");
    let store=NativeStateStore::open(path.join("state.json")).unwrap();
    store.create(&NativeAuthoritativeState::empty_from_legacy("11".repeat(32),0)).unwrap();
    let before=spotarb_native_market_core::private_https::wall_ms().saturating_sub(5_000);
    let opener=Micro5PreEntryGet{wallet:start_usdt.to_owned(),updated:before-1_000};
    let bin=spotarb_native_market_core::private_rest::BinanceCredentials::new(
        "FAKEBINKEY","FAKEBINSECRET").unwrap();
    let (opening,_)=store.audit_and_record_pre_entry_binance_baseline_once(
        &opener,1,&bin,before-1,10_000,7).unwrap();
    assert_eq!(opening.generation,2);
    let one=decimal_to_atoms("1").unwrap();
    let econ=PositionEconomicsState{entry:EntryEconomicsState{
        signal_gross_microbps:100_000_000,
        signal_expected_net_microbps:80_000_000,
        signal_spot_price_atoms:Some(decimal_to_atoms("100").unwrap()),
        signal_futures_price_atoms:Some(decimal_to_atoms("101").unwrap()),
        projected_cost_reserve_microbps:20_000_000,
        spot:LegFillAggregate::new("okx","BNT/USDT","buy",one).unwrap(),
        futures:LegFillAggregate::new("binance","BNTUSDT","sell",one).unwrap(),
        timing:EntryTiming::default(),
        post_leg1_projected_gross_microbps:None,
        post_leg1_projected_net_microbps:None,
        locked_actual_gross_microbps:None,
        locked_actual_net_microbps:None,
        signal_to_leg1_slippage_microbps:None,
        signal_to_hedge_decay_microbps:None,
        execution_cost_vs_signal_microbps:None,
    },exit:None};
    let plan=NativeEntryPlan{
        base:"BNT".into(),spot_symbol:"BNT/USDT".into(),
        futures_symbol:"BNTUSDT".into(),
        created_at_ms:before+1,
        planned_qty_atoms_e18:1_000_000_000_000_000_000,
        order_quote_atoms_e18:10_000_000_000_000_000_000,
        spot_baseline_atoms_e18:125_000_000_000_000_000,
        safety_generation:7,
    };
    let (_,permit)=store.begin_native_entry(2,plan,econ,
        "r32d6c2-spot","sab00000000000000000000000000791").unwrap();
    let state=store.record_spot_entry_observation(
        permit,1_000_000_000_000_000_000,
        Some(decimal_to_atoms("100").unwrap()),true,false,false).unwrap();
    assert_eq!(state.entry.unwrap().phase,NativeEntryPhase::SpotExposed);
    (path,store,state.generation)
}

fn micro5_wallet_bridge_seed(start_wallet:&str)->(
 std::path::PathBuf,NativeStateStore,C7LinkedExchange,u64
){
 use spotarb_native_market_core::{
    authoritative_state::{NativeSpotFirstLegWrite,NativeSpotBillsWrite},
    private_rest::{OrderObservation,RestOutcomeClass,BinanceCredentials},
 };
 let (dir,store,gen)=micro5_prepared_with_wallet(start_wallet);
 let origin=store.load().unwrap().unwrap().entry.unwrap().plan.created_at_ms;
 let client="micro5-bridge-binance-client";
 let exec=C7LinkedExchange::new(origin,client);
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let now=spotarb_native_market_core::private_https::wall_ms();
 let first=audit_okx_first_leg_before_hedge(
    &exec,&store,gen,&okx,now,OkxDomain::Global,1).unwrap();
 let (one,write)=store.record_native_spot_first_leg_evidence(first).unwrap();
 assert_eq!(write,NativeSpotFirstLegWrite::Recorded);
 let bills=audit_okx_first_leg_cash_bills_window(
    &exec,&store,one.generation,&okx,now,OkxDomain::Global,1).unwrap();
 let (two,write)=store.record_native_spot_prehedge_bills_evidence(bills).unwrap();
 assert_eq!(write,NativeSpotBillsWrite::Recorded);
 let (pre,_)=store.prepare_futures_entry_hedge(
    two.generation,7,"micro5b-futures",client).unwrap();
 let placement=OrderObservation{
    class:RestOutcomeClass::Acknowledged,terminal:false,
    filled:Some("0.4".into()),status:Some("PARTIALLY_FILLED".into()),
    order_id:Some("124".into()),client_order_id:Some(client.to_owned()),
    reason:"MICRO5B_UNTRUSTED_CANDIDATE",
 };
 let (q,_)=store.quarantine_futures_dispatch_observation(
    pre.generation,&placement,now).unwrap();
 let detail=micro3_probe(&store,&exec,q.generation);
 let (cp,_,_)=store.record_quarantined_futures_get_checkpoint(detail).unwrap();
 let (done,assessment)=store.audit_and_record_quarantined_full_window_once(
    &exec,cp.generation,&okx,&bin,now,OkxDomain::Global,10_000,1,10).unwrap();
 assert!(!assessment.may_send_order);
 (dir,store,exec,done.generation)
}
#[test]
fn micro5b_exact_wallet_100_04_minus_two_0_02_fees_matches_100_and_stays_denied(){
 use spotarb_native_market_core::{
    native_wallet_income_bridge::NativeWalletBridgeWrite,
    authoritative_state::NativeStateStore,
 };
 let (dir,store,exec,gen)=micro5_wallet_bridge_seed("100.04");
 let pre=std::fs::read(dir.join("state.json")).unwrap();
 let (final_state,status)=store.record_quarantined_wallet_income_bridge(gen).unwrap();
 assert_eq!(status,NativeWalletBridgeWrite::Recorded);
 assert_eq!(final_state.generation,gen+1);
 let witness=final_state.entry.as_ref().unwrap().wallet_income_bridge.as_ref().unwrap();
 assert_eq!(witness.start_wallet_e12(),decimal_to_atoms("100.04").unwrap());
 assert_eq!(witness.observed_wallet_e12(),decimal_to_atoms("100").unwrap());
 assert_eq!(witness.known_futures_commissions_e12(),decimal_to_atoms("0.04").unwrap() as i128);
 assert_eq!(witness.observed_wallet_delta_e12(),-(decimal_to_atoms("0.04").unwrap() as i128));
 assert!(!witness.may_send_order()&&!witness.is_lifetime_complete());
 assert_eq!(final_state.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
 assert_eq!(final_state.entry.as_ref().unwrap().futures_filled_atoms_e18,0);
 assert!(final_state.active.is_none());
 assert_ne!(std::fs::read(dir.join("state.json")).unwrap(),pre);
 let reload=NativeStateStore::open(dir.join("state.json")).unwrap();
 assert_eq!(reload.load_for_recovery().unwrap().unwrap(),final_state);
 let old=std::fs::read(dir.join("state.json")).unwrap();
 let (repeat,kind)=reload.record_quarantined_wallet_income_bridge(gen+1).unwrap();
 assert_eq!(kind,NativeWalletBridgeWrite::Idempotent);
 assert_eq!(repeat,final_state);
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),old);
 assert!(reload.prepare_futures_entry_hedge(gen+1,7,"not-allowed","not-allowed").is_err());
 assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro5b_wallet_unchanged_despite_paid_commissions_declined_before_disk_write(){
 let (dir,store,exec,gen)=micro5_wallet_bridge_seed("100");
 let original=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.record_quarantined_wallet_income_bridge(gen).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
 assert!(store.load().unwrap().unwrap().entry.unwrap().wallet_income_bridge.is_none());
 assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro5b_missing_original_opening_wallet_or_stale_writer_fails_closed(){
 let (dir,store,exec,gen)=micro4_seed("micro5b-no-opening-baseline");
 let bin=spotarb_native_market_core::private_rest::BinanceCredentials::new(
    "FAKEBINKEY","FAKEBINSECRET").unwrap();
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let (state,_)=store.audit_and_record_quarantined_full_window_once(
    &exec,gen,&okx,&bin,spotarb_native_market_core::private_https::wall_ms(),
    OkxDomain::Global,10_000,1,10).unwrap();
 let original=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.record_quarantined_wallet_income_bridge(state.generation).is_err());
 assert_eq!(original,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_,gen)=micro5_wallet_bridge_seed("100.04");
 let original=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.record_quarantined_wallet_income_bridge(gen-1).is_err());
 assert_eq!(original,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro5b_sticky_wallet_bridge_cannot_be_erased_forged_or_rebound_under_cas(){
 let (dir,store,_,gen)=micro5_wallet_bridge_seed("100.04");
 let (good,_)=store.record_quarantined_wallet_income_bridge(gen).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 for case in ["erase","drop-entry","fake-wallet","fake-fee","fake-income-complete",
              "fake-antirollback","fake-hedged","fake-source"]{
   let mut edited=good.clone();
   let e=edited.entry.as_mut().unwrap();
   match case{
     "erase"=>e.wallet_income_bridge=None,
     "drop-entry"=>edited.entry=None,
     _=>{
       let mut x=serde_json::to_value(e.wallet_income_bridge.as_ref().unwrap()).unwrap();
       match case{
         "fake-wallet"=>x["start_wallet_e12"]=serde_json::json!(1),
         "fake-fee"=>x["verified_futures_commissions_e12"]=serde_json::json!(0),
         "fake-income-complete"=>x["start_to_first_trade_global_income_history_complete"]=serde_json::json!(true),
         "fake-antirollback"=>x["independent_anti_rollback_proven"]=serde_json::json!(true),
         "fake-hedged"=>x["may_verify_hedged"]=serde_json::json!(true),
         "fake-source"=>x["pre_entry_hash"]=serde_json::json!("abcd"),
         _=>unreachable!(),
       }
       e.wallet_income_bridge=Some(serde_json::from_value(x).unwrap());
     }
   }
   edited.generation=good.generation+1;
   assert!(store.replace(good.generation,&edited).is_err(),"tampered {case} accepted");
   assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
 }
 assert!(store.transact(good.generation,|x|{
   x.entry.as_mut().unwrap().wallet_income_bridge=None;Ok(())
 }).is_err());
 let mut bypass=good.clone();
 bypass.entry.as_mut().unwrap().wallet_income_bridge=None;
 assert!(store.persist_with_failpoint(
    &bypass,spotarb_native_market_core::authoritative_state::StatePersistFailpoint::None
 ).is_err(),"legacy diagnostic writer cannot erase original bank bridge");
 assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro5b_disk_failure_preserves_original_c5_c6_and_retries(){
 let (dir,store,_,gen)=micro5_wallet_bridge_seed("100.04");
 let previous=std::fs::read(dir.join("state.json")).unwrap();
 let temp=dir.join("state.tmp");
 std::fs::create_dir(&temp).unwrap();
 assert!(store.record_quarantined_wallet_income_bridge(gen).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),previous);
 std::fs::remove_dir(&temp).unwrap();
 let (result,_)=store.record_quarantined_wallet_income_bridge(gen).unwrap();
 assert_eq!(result.generation,gen+1);
 assert!(result.entry.as_ref().unwrap().wallet_income_bridge.is_some());
 std::fs::remove_dir_all(dir).unwrap();
}


// MICRO5B2A: exact no-symbol/no-incomeType-filter Binance Futures history
// from PRE-entry wallet capture to native plan creation (a bounded 2-GET
// observation, NOT proof of SAPI transfers or historical posting finality).
struct Micro5B2GapGet{
 calls:AtomicUsize,
 flaw:usize,
 start_ms:u64,
 end_ms:u64,
}
impl Micro5B2GapGet{
 fn new(flaw:usize,start_ms:u64,end_ms:u64)->Self{
    Self{calls:AtomicUsize::new(0),flaw,start_ms,end_ms}
 }
 fn count(&self)->usize{self.calls.load(Ordering::SeqCst)}
}
impl HttpExecutor for Micro5B2GapGet{
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
    assert_eq!(req.method,HttpMethod::Get);
    assert!(!req.mutation,"income pre-entry source never permits mutation");
    let url=url::Url::parse(&req.url).unwrap();
    assert_eq!(url.host_str(),Some("fapi.binance.com"));
    assert_eq!(url.path(),"/fapi/v1/income");
    let qs=url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
    assert_eq!(qs.get("startTime").unwrap().as_ref(),
        self.start_ms.to_string());
    assert_eq!(qs.get("endTime").unwrap().as_ref(),
        self.end_ms.to_string());
    assert_eq!(qs.get("limit").unwrap().as_ref(),"1000");
    assert_eq!(qs.get("page").unwrap().as_ref(),"1");
    assert!(!qs.contains_key("symbol")&&!qs.contains_key("incomeType"));
    let n=self.calls.fetch_add(1,Ordering::SeqCst);
    let income=|ty:&str,val:&str|{
        format!(r#"[{{"symbol":"","incomeType":"{ty}","income":"{val}","asset":"USDT","time":{},"tranId":111}}]"#,
            self.start_ms)
    };
    let (code,body)=match self.flaw{
        1 if n==0=>(200,income("FUNDING_FEE","0.001")),
        2 if n==1=>(200,income("TRANSFER","10")),
        3=>(200,"not-json".to_string()),
        4=>(403,r#"{"code":-2015,"msg":"invalid API key"}"#.to_owned()),
        5=>(200,r#"{"data":[]}"#.to_owned()),
        6 if n==0=>(200,income("COMMISSION","0")),
        7 if n==1=>(200,income("REALIZED_PNL","-0.12")),
        8 if n==0=>(200,income("UNKNOWN_TYPE","0")),
        9 if n==0=>(200,"[{},{}]".to_string()),
        _=>(200,"[]".to_string()),
    };
    Ok(HttpResponse{status:code,headers:vec![],body})
 }
}
fn micro5b2_seed(tag:&str)->(std::path::PathBuf,NativeStateStore,u64,u64,u64){
 let (dir,store,_,gen)=micro5_wallet_bridge_seed("100.04");
 let (saved,_)=store.record_quarantined_wallet_income_bridge(gen).unwrap();
 let before=saved.pre_entry_binance_baseline.as_ref().unwrap().captured_at_ms();
 let entry_time=saved.entry.as_ref().unwrap().plan.created_at_ms;
 assert!(entry_time>=before&&entry_time-before<=60_000);
 let _=tag;
 (dir,store,saved.generation,before,entry_time)
}
#[test]
fn micro5b2_double_unfiltered_signed_income_gap_empty_commit_reload_no_order_permit(){
 use spotarb_native_market_core::{
     native_pre_entry_income_gap::NativePreEntryGapWrite,
     private_rest::BinanceCredentials,
 };
 let (dir,store,gen,beg,end)=micro5b2_seed("empty");
 let http=Micro5B2GapGet::new(0,beg,end);
 let creds=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let original=std::fs::read(dir.join("state.json")).unwrap();
 let (saved,kind)=store.audit_and_record_pre_entry_income_gap_once(
     &http,gen,&creds,spotarb_native_market_core::private_https::wall_ms(),10_000).unwrap();
 assert_eq!(kind,NativePreEntryGapWrite::Recorded);
 assert_eq!(saved.generation,gen+1);
 assert_eq!(http.count(),2,"two independent signed unfiltered GETs");
 let gap=saved.entry.as_ref().unwrap().pre_entry_income_gap_proof.as_ref().unwrap();
 assert_eq!(gap.source_generation(),gen);
 assert_eq!(gap.window(),(beg,end));
 assert!(!gap.may_send_order());
 assert!(!gap.all_account_history_complete()&&!gap.transfer_history_proven());
 assert_eq!(saved.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
 assert_eq!(saved.entry.as_ref().unwrap().futures_filled_atoms_e18,0);
 assert!(saved.active.is_none());
 assert_ne!(std::fs::read(dir.join("state.json")).unwrap(),original);
 let reopened=NativeStateStore::open(dir.join("state.json")).unwrap();
 assert_eq!(reopened.load_for_recovery().unwrap().unwrap(),saved);
 let before_count=http.count();
 let (repeat,status)=reopened.audit_and_record_pre_entry_income_gap_once(
     &http,gen+1,&creds,spotarb_native_market_core::private_https::wall_ms(),10_000).unwrap();
 assert_eq!(status,NativePreEntryGapWrite::Idempotent);
 assert_eq!(repeat,saved);
 assert_eq!(http.count(),before_count,"replay must NOT issue more GETs");
 assert!(reopened.prepare_futures_entry_hedge(gen+1,7,"retry","retry-client").is_err());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro5b2_funding_transfers_commission_zero_and_delayed_income_always_fail_closed(){
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 for (why,fault) in [("funding",1),("transfer",2),("garbled",3),
      ("permission",4),("wrong-json-shape",5),("zero-commission",6),
      ("negative-pnl",7),("unknown-income-type",8),("full-page",9)]{
   let (dir,store,gen,beg,end)=micro5b2_seed(why);
   let http=Micro5B2GapGet::new(fault,beg,end);
   let creds=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
   let before=std::fs::read(dir.join("state.json")).unwrap();
   assert!(store.audit_and_record_pre_entry_income_gap_once(
       &http,gen,&creds,spotarb_native_market_core::private_https::wall_ms(),
       10_000).is_err(),"unexpected income event {why} accepted");
   assert!(http.count()<=2);
   assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
   let e=store.load().unwrap().unwrap().entry.unwrap();
   assert!(e.pre_entry_income_gap_proof.is_none());
   std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn micro5b2_stale_generation_and_missing_orig_source_reject_before_any_get(){
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let (dir,store,gen,beg,end)=micro5b2_seed("stale");
 let api=Micro5B2GapGet::new(0,beg,end);
 let creds=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let snapshot=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.audit_and_record_pre_entry_income_gap_once(
   &api,gen-1,&creds,spotarb_native_market_core::private_https::wall_ms(),10_000).is_err());
 assert_eq!(api.count(),0);
 assert_eq!(snapshot,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir_all(dir).unwrap();

 let (dir,store,_,gen)=micro5_wallet_bridge_seed("100.04");
 let orig=store.load().unwrap().unwrap();
 let b=orig.pre_entry_binance_baseline.as_ref().unwrap().captured_at_ms();
 let e=orig.entry.as_ref().unwrap().plan.created_at_ms;
 let http=Micro5B2GapGet::new(0,b,e);
 assert!(store.audit_and_record_pre_entry_income_gap_once(
   &http,gen,&creds,spotarb_native_market_core::private_https::wall_ms(),10_000).is_err(),
   "wallet-income-bridge prerequisite missing");
 assert_eq!(http.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro5b2_owner_cas_and_debug_writer_refuse_gap_erasure_forgery_and_source_rebinding(){
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let (dir,store,gen,beg,end)=micro5b2_seed("forgery");
 let api=Micro5B2GapGet::new(0,beg,end);
 let creds=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let (saved,_)=store.audit_and_record_pre_entry_income_gap_once(
     &api,gen,&creds,spotarb_native_market_core::private_https::wall_ms(),10_000).unwrap();
 let bytes=std::fs::read(dir.join("state.json")).unwrap();
 for kind in ["erase","erase-entry","wrong-window","replace-baseline-hash",
              "replace-wallet-bridge-hash","fake-all-account-history","fake-antirollback",
              "fake-transfers","fake-send","fake-recovery","fake-hedged","false-reads"]{
   let mut altered=saved.clone();
   let e=altered.entry.as_mut().unwrap();
   match kind{
      "erase"=>e.pre_entry_income_gap_proof=None,
      "erase-entry"=>altered.entry=None,
      _=>{
         let mut data=serde_json::to_value(e.pre_entry_income_gap_proof.as_ref().unwrap()).unwrap();
         match kind{
            "wrong-window"=>data["window_end_ms"]=serde_json::json!(999),
            "replace-baseline-hash"=>data["baseline_sha256"]=serde_json::json!("fake"),
            "replace-wallet-bridge-hash"=>data["wallet_bridge_sha256"]=serde_json::json!("fake"),
            "fake-all-account-history"=>data["all_account_history_complete"]=serde_json::json!(true),
            "fake-antirollback"=>data["external_anti_rollback_proven"]=serde_json::json!(true),
            "fake-transfers"=>data["global_internal_external_transfers_complete"]=serde_json::json!(true),
            "fake-send"=>data["may_send_order"]=serde_json::json!(true),
            "fake-recovery"=>data["may_release_recovery"]=serde_json::json!(true),
            "fake-hedged"=>data["may_promote_hedged"]=serde_json::json!(true),
            "false-reads"=>data["unfiltered_income_snapshots"]=serde_json::json!(1),
            _=>unreachable!(),
         }
         e.pre_entry_income_gap_proof=Some(serde_json::from_value(data).unwrap());
      }
   }
   altered.generation=saved.generation+1;
   assert!(store.replace(saved.generation,&altered).is_err(),"{kind} forged");
   assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
 }
 let mut erased=saved.clone();erased.entry.as_mut().unwrap().pre_entry_income_gap_proof=None;
 assert!(store.persist_with_failpoint(
     &erased,spotarb_native_market_core::authoritative_state::StatePersistFailpoint::None
 ).is_err(),"diagnostic state writer cannot delete opening-income proof");
 assert!(store.transact(saved.generation,|x|{
     x.entry.as_mut().unwrap().pre_entry_income_gap_proof=None;Ok(())
 }).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro5b2_failed_disk_write_does_not_erase_opening_cashflow_and_retry_passes(){
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let (dir,store,gen,beg,end)=micro5b2_seed("fsync");
 let api=Micro5B2GapGet::new(0,beg,end);
 let creds=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let snapshot=std::fs::read(dir.join("state.json")).unwrap();
 let blocker=dir.join("state.tmp");std::fs::create_dir(&blocker).unwrap();
 assert!(store.audit_and_record_pre_entry_income_gap_once(
     &api,gen,&creds,spotarb_native_market_core::private_https::wall_ms(),10_000).is_err());
 assert_eq!(snapshot,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir(&blocker).unwrap();
 let (saved,_)=store.audit_and_record_pre_entry_income_gap_once(
     &api,gen,&creds,spotarb_native_market_core::private_https::wall_ms(),10_000).unwrap();
 assert_eq!(saved.generation,gen+1);
 assert!(saved.entry.as_ref().unwrap().pre_entry_income_gap_proof.is_some());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn micro5b2_racing_gap_observers_only_one_owner_generation_can_commit(){
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let (dir,store,gen,beg,end)=micro5b2_seed("race");
 let api=Micro5B2GapGet::new(0,beg,end);
 let creds=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 std::thread::scope(|scope|{
   let a=scope.spawn(||store.audit_and_record_pre_entry_income_gap_once(
       &api,gen,&creds,spotarb_native_market_core::private_https::wall_ms(),10_000));
   let b=scope.spawn(||store.audit_and_record_pre_entry_income_gap_once(
       &api,gen,&creds,spotarb_native_market_core::private_https::wall_ms(),10_000));
   let rs=[a.join().unwrap(),b.join().unwrap()];
   assert_eq!(rs.iter().filter(|x|x.is_ok()).count(),1);
   assert_eq!(rs.iter().filter(|x|x.is_err()).count(),1);
 });
 let state=store.load().unwrap().unwrap();
 assert_eq!(state.generation,gen+1);
 assert!(state.entry.unwrap().pre_entry_income_gap_proof.is_some());
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE1: operator-only reduction review on the exact MICRO1/MICRO3/C5/C6
// signed source, NEVER a sell order permission or assumed verified close.
fn trade1_market(now:u64)->spotarb_native_market_core::market_metadata::NativeMarketMetadata{
    use spotarb_native_market_core::market_metadata::NativeMarketMetadata;
    NativeMarketMetadata{
        base:"BNT".into(),spot_symbol:"BNT-USDT".into(),future_symbol:"BNTUSDT".into(),
        spot_min_amount:Some("0.1".into()),future_min_base:Some("0.1".into()),
        spot_min_cost:"0".into(),future_min_cost:"5".into(),
        spot_step:Some("0.1".into()),future_step_base:Some("0.1".into()),
        contract_size:"1".into(),
        buy_exchange:"okx_spot".into(),sell_exchange:"binance_futures".into(),
        route_label:spotarb_native_market_core::market_metadata::ROUTE_LABEL.into(),
        static_reason:None,
        loaded_ns:123,loaded_wall_ms:now-1000,
        spot_tick:Some("0.01".into()),future_tick:Some("0.01".into()),
        spot_status:"live".into(),future_status:"TRADING".into(),
        spot_max_amount:Some("2000".into()),future_max_base:Some("2000".into()),
    }
}
fn trade1_quarantine_c6()->(std::path::PathBuf,NativeStateStore,C7LinkedExchange,u64){
    use spotarb_native_market_core::private_rest::BinanceCredentials;
    let (dir,store,exec,gen)=micro4_seed("trade1-futures-residual");
    let now=spotarb_native_market_core::private_https::wall_ms();
    let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let (state,_)=store.audit_and_record_quarantined_full_window_once(
        &exec,gen,&okx,&bin,now,OkxDomain::Global,10_000,1,10).unwrap();
    (dir,store,exec,state.generation)
}
#[test]
fn trade1_residual_0_6_against_short_0_4_requires_operator_even_when_size_grid_valid(){
    use spotarb_native_market_core::{
        native_micro_trade_size::{preview_residual_spot_reduction,ResidualSpotStatus},
        authoritative_state::NativeStateStore,
    };
    let (dir,store,exec,gen)=trade1_quarantine_c6();
    let now=spotarb_native_market_core::private_https::wall_ms();
    let m=trade1_market(now);
    let saved=store.load().unwrap().unwrap();
    assert_eq!(saved.generation,gen);
    let status=preview_residual_spot_reduction(&saved,&m,now,decimal_to_atoms("20").unwrap()).unwrap();
    assert_eq!(status.spot_total_e12,decimal_to_atoms("1").unwrap());
    assert_eq!(status.futures_short_e12,decimal_to_atoms("0.4").unwrap());
    assert_eq!(status.unmatched_spot_e12,decimal_to_atoms("0.6").unwrap());
    assert_eq!(status.estimated_spot_sell_quote_e12,decimal_to_atoms("12").unwrap());
    assert_eq!(status.status,ResidualSpotStatus::ExactLotCandidateOnly);
    assert!(!status.may_send_order&&!status.may_auto_close
        &&!status.may_mark_verified_closed);
    let reopened=NativeStateStore::open(dir.join("state.json")).unwrap();
    assert_eq!(reopened.load().unwrap().unwrap(),saved);
    assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade1_residual_less_than_exchange_min_or_misaligned_grid_not_actionable(){
    use spotarb_native_market_core::native_micro_trade_size::{
        preview_residual_spot_reduction,ResidualSpotStatus,
    };
    let (dir,store,_,_)=trade1_quarantine_c6();
    let s=store.load().unwrap().unwrap();
    let now=spotarb_native_market_core::private_https::wall_ms();
    let mut m=trade1_market(now);
    m.spot_min_cost="15".into();
    let x=preview_residual_spot_reduction(&s,&m,now,decimal_to_atoms("20").unwrap()).unwrap();
    assert_eq!(x.status,ResidualSpotStatus::ManualDustOrBelowMinimum);
    assert!(!x.may_send_order);
    m.spot_min_cost="0".into();m.spot_step=Some("0.4".into());
    let x=preview_residual_spot_reduction(&s,&m,now,decimal_to_atoms("20").unwrap()).unwrap();
    assert_eq!(x.status,ResidualSpotStatus::ManualGridMismatch);
    assert!(!x.may_auto_close);
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade1_unverified_futures_trades_cannot_invent_a_manual_sell_candidate(){
    use spotarb_native_market_core::native_micro_trade_size::preview_residual_spot_reduction;
    let (dir,store,_,gen)=micro4_seed("trade1-no-c5-c6");
    let now=spotarb_native_market_core::private_https::wall_ms();
    let before=std::fs::read(dir.join("state.json")).unwrap();
    let bad=preview_residual_spot_reduction(&store.load().unwrap().unwrap(),
        &trade1_market(now),now,decimal_to_atoms("20").unwrap());
    assert!(bad.is_err(),"unverified Binance short should not yield recovery candidate");
    assert_eq!(store.load().unwrap().unwrap().generation,gen);
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
    std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2A: signed-source-backed manual recovery triage, no POST/permission.
#[test]
fn trade2_supervisor_partial_0_6_original_needs_all_four_live_rechecks(){
 use spotarb_native_market_core::native_supervised_recovery::{
    preview_supervised_recovery,RecoveryTriage,
 };
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let now=spotarb_native_market_core::private_https::wall_ms();
 let market=trade1_market(now);
 let disk=std::fs::read(dir.join("state.json")).unwrap();
 let before_get=exec.total();
 let triage=preview_supervised_recovery(&store,gen,
     &market,now,decimal_to_atoms("20").unwrap()).unwrap();
 assert_eq!(triage.native_generation,gen);
 assert!(triage.original_source_sha256.len()==64);
 assert_eq!(triage.original_futures_order_id,"124");
 assert_eq!(triage.observed_short_e12,decimal_to_atoms("0.4").unwrap());
 assert_eq!(triage.unhedged_spot_e12,decimal_to_atoms("0.6").unwrap());
 assert_eq!(triage.estimate_spot_sell_quote_e12,decimal_to_atoms("12").unwrap());
 assert_eq!(triage.triage,RecoveryTriage::PartialSpotReduceOperatorOnly);
 assert!(triage.must_recheck_open_orders
    &&triage.must_recheck_futures_position
    &&triage.must_recheck_available_spot_balance
    &&triage.must_recheck_all_partial_fills_and_fees);
 assert!(!triage.may_send_order&&!triage.may_auto_hedge
    &&!triage.may_auto_unwind&&!triage.may_verify_closed
    &&!triage.may_release_cooldown);
 assert!(!triage.actual_transfer_and_funding_history_complete
    &&!triage.current_account_flatness_proven
    &&!triage.actual_orderbook_vwap_proven);
 assert_eq!(disk,std::fs::read(dir.join("state.json")).unwrap());
 assert_eq!(exec.total(),before_get,"triage never performs HTTP");
 assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2_supervisor_stale_account_or_stale_market_denies_even_when_previous_checks_pass(){
 use spotarb_native_market_core::native_supervised_recovery::{
    preview_supervised_recovery,RecoveryTriageError,MAX_RECOVERY_SOURCE_AGE_MS,
 };
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let saved=store.load().unwrap().unwrap();
 let at=saved.entry.as_ref().unwrap()
    .prospective_joint_evidence.as_ref().unwrap().captured_at_ms;
 let later=at+MAX_RECOVERY_SOURCE_AGE_MS+1;
 let market=trade1_market(later);
 assert_eq!(preview_supervised_recovery(&store,gen,&market,later,
    decimal_to_atoms("20").unwrap()),Err(RecoveryTriageError::StaleAccountAudit));
 let now=spotarb_native_market_core::private_https::wall_ms();
 let mut stale_market=trade1_market(now);
 stale_market.loaded_wall_ms=now-15_000;
 assert_eq!(preview_supervised_recovery(&store,gen,&stale_market,now,
    decimal_to_atoms("20").unwrap()),Err(RecoveryTriageError::StaleMarket));
 assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2_supervisor_refuses_stale_generation_foreign_market_and_incomplete_get_source(){
 use spotarb_native_market_core::native_supervised_recovery::{
    preview_supervised_recovery,RecoveryTriageError,
 };
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let now=spotarb_native_market_core::private_https::wall_ms();
 let m=trade1_market(now);
 let old=std::fs::read(dir.join("state.json")).unwrap();
 assert_eq!(preview_supervised_recovery(&store,gen-1,&m,now,
    decimal_to_atoms("20").unwrap()),Err(RecoveryTriageError::StaleGeneration));
 let mut foreign=m.clone();foreign.base="ZEC".into();
 assert_eq!(preview_supervised_recovery(&store,gen,&foreign,now,
    decimal_to_atoms("20").unwrap()),Err(RecoveryTriageError::WrongIdentity));
 assert_eq!(old,std::fs::read(dir.join("state.json")).unwrap());
 assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,exec,gen)=micro4_seed("trade2-no-proven-binance-account");
 assert_eq!(preview_supervised_recovery(&store,gen,&m,now,
    decimal_to_atoms("20").unwrap()),Err(RecoveryTriageError::MissingOrIncomplete));
 assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2_supervisor_dust_or_untradable_spot_says_manual_and_never_rounds(){
 use spotarb_native_market_core::native_supervised_recovery::{
    preview_supervised_recovery,RecoveryTriage,
 };
 let (dir,store,_,gen)=trade1_quarantine_c6();
 let now=spotarb_native_market_core::private_https::wall_ms();
 let mut m=trade1_market(now);
 m.spot_min_cost="15".into();
 let small=preview_supervised_recovery(&store,gen,&m,now,
    decimal_to_atoms("20").unwrap()).unwrap();
 assert_eq!(small.triage,RecoveryTriage::SmallOrOffGridResidualManual);
 assert!(!small.may_send_order);
 m.spot_min_cost="0".into();m.spot_step=Some("0.4".into());
 let wrong=preview_supervised_recovery(&store,gen,&m,now,
    decimal_to_atoms("20").unwrap()).unwrap();
 assert_eq!(wrong.triage,RecoveryTriage::SmallOrOffGridResidualManual);
 assert_eq!(wrong.unhedged_spot_e12,small.unhedged_spot_e12,
     "do not round away the unmatched exposure");
 assert!(!wrong.may_auto_unwind);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2B signed double-bracket GET fixture. Only the missing OKX
// all-SPOT-pending endpoint and explicit adversarial faults are wrapped;
// remaining original order, balance and position replies come from C7.
struct Trade2BFreshGet{
 inner:C7LinkedExchange,mode:usize,
 calls:AtomicUsize,binance_order_reads:AtomicUsize,
}
impl Trade2BFreshGet{
 fn new(inner:C7LinkedExchange,mode:usize)->Self{
   Self{inner,mode,calls:AtomicUsize::new(0),
        binance_order_reads:AtomicUsize::new(0)}
 }
 fn count(&self)->usize{self.calls.load(Ordering::SeqCst)}
}
impl HttpExecutor for Trade2BFreshGet {
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
   assert_eq!(req.method,HttpMethod::Get);
   assert!(!req.mutation&&req.body.is_none());
   self.calls.fetch_add(1,Ordering::SeqCst);
   let u=url::Url::parse(&req.url).unwrap();
   let path=u.path();
   if path=="/fapi/v1/order"{
     self.binance_order_reads.fetch_add(1,Ordering::SeqCst);
   }
   // The second Binance ORIGINAL order read starts the second full
   // bracket. Keep the "later" state sticky for every subsequent GET.
   let later=self.binance_order_reads.load(Ordering::SeqCst)>1;
   let changed:Option<(u16,String)>=match (self.mode,path){
     (1,"/fapi/v1/openOrders")=>
       Some((200,r#"[{"symbol":"BNTUSDT","orderId":555,"clientOrderId":"unexpected","side":"SELL","positionSide":"BOTH","status":"NEW","reduceOnly":false}]"#.into())),
     (2,"/api/v5/trade/orders-pending")=>
       Some((200,r#"{"code":"0","data":[{"instId":"BNT-USDT","ordId":"555","clOrdId":"unexpected","state":"live","side":"sell","sz":"0.6","accFillSz":"0"}]}"#.into())),
     (3,"/fapi/v3/positionRisk")=>
       Some((200,r#"[{"symbol":"BNTUSDT","positionAmt":"-0.5","positionSide":"BOTH","marginAsset":"USDT","updateTime":1780000000400}]"#.into())),
     (4,"/api/v5/account/balance")=>
       Some((200,r#"{"code":"0","data":[{"details":[{"ccy":"BNT","cashBal":"1.125","availBal":"0.525","frozenBal":"0.6","uTime":"1780000000400"}]}]}"#.into())),
     (5,"/fapi/v3/balance") if later=>
       Some((200,r#"[{"asset":"USDT","balance":"99.9","availableBalance":"99","updateTime":1780000000600}]"#.into())),
     (6,"/fapi/v1/order") if later=>
       Some((200,format!(r#"{{"orderId":124,"clientOrderId":"{}","status":"FILLED","executedQty":"0.5","avgPrice":"101","cumQuote":"50.5"}}"#,self.inner.client))),
     (7,"/fapi/v1/order")=>
       Some((200,format!(r#"{{"orderId":124,"clientOrderId":"{}","status":"NEW","executedQty":"0.4","avgPrice":"101","cumQuote":"40.4"}}"#,self.inner.client))),
     (8,"/fapi/v1/order")=>
       Some((200,format!(r#"{{"orderId":124,"clientOrderId":"{}","status":"CANCELED","executedQty":"0.5","avgPrice":"101","cumQuote":"50.5"}}"#,self.inner.client))),
     (9,"/api/v5/trade/order")=>
       Some((200,r#"{"code":"0","data":[{"ordId":"789","clOrdId":"sab00000000000000000000000000791","state":"filled","accFillSz":"1.2","avgPx":"100"}]}"#.into())),
     (10,"/fapi/v3/positionRisk")=>
       Some((200,r#"[{"symbol":"BNTUSDT","positionAmt":"-0.4","positionSide":"BOTH","marginAsset":"USDT","updateTime":1780000000400},{"symbol":"ZECUSDT","positionAmt":"0.01","positionSide":"BOTH","marginAsset":"USDT"}]"#.into())),
     (11,"/fapi/v1/accountConfig")=>Some((200,r#"{"multiAssetsMargin":true}"#.into())),
     (12,"/fapi/v3/balance")=>Some((200,r#"{"bad":"json envelope"}"#.into())),
     (13,"/fapi/v1/openOrders")=>Some((403,r#"{"code":-2015}"#.into())),
     (14,"/fapi/v1/openOrders") if later=>
       Some((200,r#"[{"symbol":"BNTUSDT","orderId":555,"clientOrderId":"late","side":"BUY","positionSide":"BOTH","status":"NEW","reduceOnly":true}]"#.into())),
     (15,"/fapi/v3/balance")=>Some((200,r#"[{"asset":"USDT","balance":"100","availableBalance":"120","updateTime":1780000000600}]"#.into())),
     (16,"/api/v5/account/positions")=>
       Some((200,r#"{"code":"0","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","pos":"0.01","posSide":"net"}]}"#.into())),
     (17,"/fapi/v1/positionSide/dual")=>Some((200,r#"{"dualSidePosition":true}"#.into())),
     (18,"/api/v5/account/config")=>
       Some((200,r#"{"code":"0","data":[{"acctLv":"3","posMode":"long_short_mode"}]}"#.into())),
     (19,"/fapi/v1/openOrders")=>
       Some((200,r#"[{"symbol":"ZECUSDT","orderId":333,"clientOrderId":"foreign","side":"BUY","positionSide":"BOTH","status":"NEW","reduceOnly":true}]"#.into())),
     (20,"/fapi/v3/positionRisk")=>
       Some((200,r#"[{"symbol":"BNTUSDT","positionAmt":"0.4","positionSide":"BOTH","marginAsset":"USDT","updateTime":1780000000400}]"#.into())),
     _=>None,
   };
   if path=="/api/v5/trade/orders-pending" && changed.is_none(){
      return Ok(HttpResponse{status:200,headers:vec![],
         body:r#"{"code":"0","data":[]}"#.into()})
   }
   if let Some((status,body))=changed{
      return Ok(HttpResponse{status,headers:vec![],body})
   }
   self.inner.execute_request(req)
 }
}
#[test]
fn trade2b_signed_22_get_fresh_original_order_and_account_brackets_are_diagnostic_only(){
 use spotarb_native_market_core::native_supervised_recovery_live::audit_fresh_supervised_recovery_once;
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let api=Trade2BFreshGet::new(exec,0);
 let now=spotarb_native_market_core::private_https::wall_ms();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let spot=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let future=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let report=audit_fresh_supervised_recovery_once(
    &api,&store,gen,&trade1_market(now),decimal_to_atoms("20").unwrap(),
    &spot,&future,now,OkxDomain::Global,10_000).unwrap();
 assert_eq!(api.count(),22);
 assert_eq!(report.read_only_signed_gets,22);
 assert_eq!(report.independent_account_brackets,2);
 assert_eq!(report.original_spot_order_filled_e12,decimal_to_atoms("1").unwrap());
 assert_eq!(report.original_futures_order_filled_e12,decimal_to_atoms("0.4").unwrap());
 assert_eq!(report.observed_net_available_spot_e12,decimal_to_atoms("1").unwrap());
 assert_eq!(report.observed_futures_short_e12,decimal_to_atoms("0.4").unwrap());
 assert_eq!(report.observed_futures_wallet_usdt_e12,decimal_to_atoms("100").unwrap());
 assert!(report.original_orders_terminal
    &&report.no_open_binance_or_spot_okx_orders_in_two_brackets
    &&report.all_snapshots_consistent);
 assert!(!report.new_fills_since_original_audit_proven_absent
    &&!report.cross_venue_atomicity_proven&&!report.real_orderbook_liquidity_proven);
 assert!(!report.may_send_order&&!report.may_auto_hedge
    &&!report.may_auto_unwind&&!report.may_verify_closed
    &&!report.may_release_cooldown);
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 assert!(store.load().unwrap().unwrap().active.is_none());
 assert_eq!(api.inner.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2b_open_order_delayed_fills_frozen_balance_modes_and_foreign_positions_fail_closed(){
 use spotarb_native_market_core::native_supervised_recovery_live::audit_fresh_supervised_recovery_once;
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 for (label,mode) in [
    ("binance-pending",1),("okx-pending",2),("different-short",3),
    ("frozen-spot",4),("changed-wallet-second-read",5),("late-original-fill",6),
    ("original-binance-nonterminal",7),("other-binance-fill",8),
    ("okx-original-fill-drift",9),("foreign-active-short",10),
    ("multiassets",11),("malformed-balance",12),("http-permission",13),
    ("pending-only-second-bracket",14),("overavailable",15),
    ("okx-unrelated-position",16),("hedge-mode",17),
    ("okx-wrong-mode",18),("foreign-binance-order",19),("long-replaces-short",20),
 ]{
    let (dir,store,exec,gen)=trade1_quarantine_c6();
    let api=Trade2BFreshGet::new(exec,mode);
    let now=spotarb_native_market_core::private_https::wall_ms();
    let before=std::fs::read(dir.join("state.json")).unwrap();
    let spot=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let future=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    assert!(audit_fresh_supervised_recovery_once(
        &api,&store,gen,&trade1_market(now),decimal_to_atoms("20").unwrap(),
        &spot,&future,now,OkxDomain::Global,10_000).is_err(),
        "dangerous account drift accepted: {label}");
    assert!(api.count()<=22);
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before,
        "state silently modified on {label}");
    assert_eq!(api.inner.mutations.load(Ordering::SeqCst),0);
    std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2b_stale_or_missing_original_source_or_bad_recvwindow_rejected_without_http(){
 use spotarb_native_market_core::native_supervised_recovery_live::audit_fresh_supervised_recovery_once;
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let api=Trade2BFreshGet::new(exec,0);
 let now=spotarb_native_market_core::private_https::wall_ms();
 let m=trade1_market(now);
 let spot=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let future=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 assert!(audit_fresh_supervised_recovery_once(
   &api,&store,gen-1,&m,decimal_to_atoms("20").unwrap(),
   &spot,&future,now,OkxDomain::Global,10_000).is_err());
 assert!(audit_fresh_supervised_recovery_once(
   &api,&store,gen,&m,decimal_to_atoms("20").unwrap(),
   &spot,&future,now,OkxDomain::Global,30_000).is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,exec,gen)=micro4_seed("trade2b-missing-c5-c6");
 let api=Trade2BFreshGet::new(exec,0);
 assert!(audit_fresh_supervised_recovery_once(
   &api,&store,gen,&m,decimal_to_atoms("20").unwrap(),
   &spot,&future,now,OkxDomain::Global,10_000).is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2b_racing_native_owner_revision_during_second_bracket_invalidates_fresh_report(){
 use spotarb_native_market_core::native_supervised_recovery_live::{
   audit_fresh_supervised_recovery_once,FreshRecoveryError,
 };
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 struct AdvancingOwner<'a>{
    wrapped:Trade2BFreshGet,
    store:&'a NativeStateStore,
    gen:u64,
    advanced:AtomicUsize,
 }
 impl HttpExecutor for AdvancingOwner<'_>{
    fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
      let url=url::Url::parse(&req.url).unwrap();
      if url.path()=="/fapi/v1/order"
         &&self.wrapped.binance_order_reads.load(Ordering::SeqCst)==1
         &&self.advanced.fetch_add(1,Ordering::SeqCst)==0{
         // External actor changes durable owner generation while 22 GET
         // recovery audit is in flight; must DENY its stale report.
         let (new_state,_)=self.store.transact(self.gen,|_|Ok(())).unwrap();
         assert_eq!(new_state.generation,self.gen+1);
      }
      self.wrapped.execute_request(req)
    }
 }
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let api=AdvancingOwner{
    wrapped:Trade2BFreshGet::new(exec,0),store:&store,
    gen,advanced:AtomicUsize::new(0),
 };
 let now=spotarb_native_market_core::private_https::wall_ms();
 let market=trade1_market(now);
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let result=audit_fresh_supervised_recovery_once(
     &api,&store,gen,&market,decimal_to_atoms("20").unwrap(),
     &okx,&bn,now,OkxDomain::Global,10_000);
 assert_eq!(result,Err(FreshRecoveryError::State(
    spotarb_native_market_core::authoritative_state::NativeStateError::Generation)));
 assert_eq!(api.advanced.load(Ordering::SeqCst),1);
 assert_eq!(api.wrapped.count(),22);
 assert_eq!(store.load().unwrap().unwrap().generation,gen+1);
 assert_eq!(api.wrapped.inner.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2C read-only replay of original per-fill prices/quantities/fees/IDs.
struct Trade2CFillGet{
    base:Trade2BFreshGet,
    fault:usize,
    total:AtomicUsize,
    binance_trade_reads:AtomicUsize,
    okx_fill_reads:AtomicUsize,
}
impl Trade2CFillGet{
    fn new(base:C7LinkedExchange,fault:usize)->Self{
        Self{base:Trade2BFreshGet::new(base,0),fault,
            total:AtomicUsize::new(0),binance_trade_reads:AtomicUsize::new(0),
            okx_fill_reads:AtomicUsize::new(0)}
    }
    fn count(&self)->usize{self.total.load(Ordering::SeqCst)}
}
impl HttpExecutor for Trade2CFillGet{
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
    assert_eq!(req.method,HttpMethod::Get);
    assert!(!req.mutation&&req.body.is_none());
    self.total.fetch_add(1,Ordering::SeqCst);
    let u=url::Url::parse(&req.url).unwrap();
    let path=u.path();
    let okx_read=if path=="/api/v5/trade/fills-history"{
        self.okx_fill_reads.fetch_add(1,Ordering::SeqCst)
    }else{0};
    let future_read=if path=="/fapi/v1/userTrades"{
        self.binance_trade_reads.fetch_add(1,Ordering::SeqCst)
    }else{0};
    let q=u.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
    let okx_first=q.get("after").is_none();
    let first=self.base.inner.spot.base_ms;
    let replace:Option<(u16,String)>=match (self.fault,path){
      (1,"/api/v5/trade/fills-history") if okx_first=>
        Some((200,format!(r#"{{"code":"0","data":[{{"ordId":"789","billId":"301","tradeId":"T1","fillPx":"100","fillSz":"1","fee":"-0.2","feeCcy":"USDT","fillTime":"{}"}}]}}"#,first+320))),
      (2,"/api/v5/trade/fills-history") if okx_first=>
        Some((200,format!(r#"{{"code":"0","data":[{{"ordId":"789","billId":"301","tradeId":"T1","fillPx":"101","fillSz":"1","fee":"-0.1","feeCcy":"USDT","fillTime":"{}"}}]}}"#,first+320))),
      (3,"/api/v5/trade/fills-history") if okx_first=>
        Some((200,format!(r#"{{"code":"0","data":[{{"ordId":"789","billId":"301","tradeId":"T1","fillPx":"100","fillSz":"0.9","fee":"-0.1","feeCcy":"USDT","fillTime":"{}"}}]}}"#,first+320))),
      (4,"/api/v5/trade/fills-history") if okx_first=>
        Some((200,format!(r#"{{"code":"0","data":[{{"ordId":"789","billId":"301","tradeId":"T1","fillPx":"100","fillSz":"1","fee":"-0.1","feeCcy":"USDT","fillTime":"{}"}}]}}"#,first+999))),
      (5,"/api/v5/trade/fills-history") if okx_first=>
        Some((200,format!(r#"{{"code":"0","data":[{{"ordId":"789","billId":"301","tradeId":"T1","fillPx":"100","fillSz":"1","fee":"-0.1","feeCcy":"USDT","fillTime":"{}"}},{{"ordId":"789","billId":"300","tradeId":"T1","fillPx":"100","fillSz":"1","fee":"-0.1","feeCcy":"USDT","fillTime":"{}"}}]}}"#,first+320,first+320))),
      (6,"/api/v5/trade/fills-history") if okx_first=>
        Some((200,r#"{"code":"0","data":[{"ordId":"124","billId":"301","tradeId":"T1","fillPx":"100","fillSz":"1","fee":"-0.1","feeCcy":"USDT","fillTime":"1780000000320"}]}"#.into())),
      (7,"/api/v5/trade/fills-history") if !okx_first=>
        Some((200,format!(r#"{{"code":"0","data":[{{"ordId":"789","billId":"300","tradeId":"T2","fillPx":"100","fillSz":"0.1","fee":"-0.01","feeCcy":"USDT","fillTime":"{}"}}]}}"#,first+330))),
      (8,"/api/v5/trade/fills-history") if okx_first=>
        Some((200,r#"{"code":"0","data":[{"ordId":"789","billId":"301","tradeId":"T1","fillPx":"100","fillSz":"1","fee":null,"feeCcy":"USDT","fillTime":"1780000000320"}]}"#.into())),
      (9,"/fapi/v1/userTrades")=>
        Some((200,format!(r#"[{{"symbol":"BNTUSDT","id":1001,"orderId":124,"side":"SELL","positionSide":"BOTH","marginAsset":"USDT","buyer":false,"realizedPnl":"0","price":"101","qty":"0.2","commission":"0.07","commissionAsset":"USDT","time":{}}},{{"symbol":"BNTUSDT","id":1002,"orderId":124,"side":"SELL","positionSide":"BOTH","marginAsset":"USDT","buyer":false,"realizedPnl":"0","price":"101","qty":"0.2","commission":"0.02","commissionAsset":"USDT","time":{}}}]"#,first+220,first+320))),
      (10,"/fapi/v1/userTrades")=>
        Some((200,format!(r#"[{{"symbol":"BNTUSDT","id":1001,"orderId":124,"side":"SELL","positionSide":"BOTH","marginAsset":"USDT","buyer":false,"realizedPnl":"0","price":"101","qty":"0.2","commission":"0.02","commissionAsset":"USDT","time":{}}}]"#,first+220))),
      (11,"/fapi/v1/userTrades")=>
        Some((200,format!(r#"[{{"symbol":"BNTUSDT","id":1001,"orderId":124,"side":"SELL","positionSide":"BOTH","marginAsset":"USDT","buyer":false,"realizedPnl":"0","price":"101","qty":"0.2","commission":"0.02","commissionAsset":"USDT","time":{}}},{{"symbol":"BNTUSDT","id":1001,"orderId":124,"side":"SELL","positionSide":"BOTH","marginAsset":"USDT","buyer":false,"realizedPnl":"0","price":"101","qty":"0.2","commission":"0.02","commissionAsset":"USDT","time":{}}}]"#,first+220,first+220))),
      (12,"/fapi/v1/userTrades")=>
        Some((200,format!(r#"[{{"symbol":"BNTUSDT","id":1001,"orderId":124,"side":"SELL","positionSide":"BOTH","marginAsset":"USDT","buyer":false,"realizedPnl":"0","price":"101","qty":"0.2","commission":"0.02","commissionAsset":"USDT","time":{}}},{{"symbol":"BNTUSDT","id":1002,"orderId":999,"side":"SELL","positionSide":"BOTH","marginAsset":"USDT","buyer":false,"realizedPnl":"0","price":"101","qty":"0.2","commission":"0.02","commissionAsset":"USDT","time":{}}}]"#,first+220,first+320))),
      (13,"/fapi/v1/userTrades")=>
        Some((200,format!(r#"[{{"symbol":"BNTUSDT","id":1001,"orderId":124,"side":"BUY","positionSide":"BOTH","marginAsset":"USDT","buyer":true,"realizedPnl":"0","price":"101","qty":"0.2","commission":"0.02","commissionAsset":"USDT","time":{}}}]"#,first+220))),
      (14,"/fapi/v1/userTrades")=>
        Some((200,r#"[{"symbol":"BNTUSDT","id":1001,"orderId":124,"side":"SELL","positionSide":"BOTH","marginAsset":"USDT","buyer":false,"realizedPnl":"0","price":"101","qty":"0.2","commission":"0.02","commissionAsset":"BNB","time":1780000000220}]"#.into())),
      (15,"/fapi/v1/userTrades") if future_read>0=>
        Some((200,format!(r#"[{{"symbol":"BNTUSDT","id":1001,"orderId":124,"side":"SELL","positionSide":"BOTH","marginAsset":"USDT","buyer":false,"realizedPnl":"0","price":"101","qty":"0.2","commission":"0.07","commissionAsset":"USDT","time":{}}},{{"symbol":"BNTUSDT","id":1002,"orderId":124,"side":"SELL","positionSide":"BOTH","marginAsset":"USDT","buyer":false,"realizedPnl":"0","price":"101","qty":"0.2","commission":"0.02","commissionAsset":"USDT","time":{}}}]"#,first+220,first+320))),
      (16,"/fapi/v1/userTrades")=>Some((403,r#"{"code":-2015}"#.into())),
      (17,"/fapi/v1/userTrades")=>Some((200,r#"{"code":0}"#.into())),
      (18,"/api/v5/trade/fills-history") if okx_first && okx_read>0=>
        Some((200,format!(r#"{{"code":"0","data":[{{"ordId":"789","billId":"301","tradeId":"T1","fillPx":"100","fillSz":"1","fee":"-0.2","feeCcy":"USDT","fillTime":"{}"}}]}}"#,first+320))),
      _=>None,
    };
    if let Some((status,body))=replace{
        return Ok(HttpResponse{status,headers:vec![],body})
    }
    self.base.execute_request(req)
 }
}
#[test]
fn trade2c_dual_22_get_account_brackets_and_twice_both_leg_trade_ids_fee_exact(){
 use spotarb_native_market_core::native_supervised_recovery_fills::
   audit_fresh_supervised_recovery_fills_once;
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let api=Trade2CFillGet::new(exec,0);
 let at=spotarb_native_market_core::private_https::wall_ms();
 let previous=std::fs::read(dir.join("state.json")).unwrap();
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let result=audit_fresh_supervised_recovery_fills_once(
   &api,&store,gen,&trade1_market(at),decimal_to_atoms("20").unwrap(),
   &okx,&bn,at,OkxDomain::Global,10_000).unwrap();
 assert_eq!(api.count(),50,"22 GET + 6 history GET + 22 GET");
 assert_eq!(result.full_original_trade_scans,2);
 assert_eq!(result.signed_get_history_reads,6);
 assert_eq!(result.spot_trade_ids,vec!["T1".to_string()]);
 assert_eq!(result.futures_trade_ids,vec!["1001".to_string(),"1002".to_string()]);
 assert_eq!(result.spot_fee_quote_e12,decimal_to_atoms("0.1").unwrap() as i128);
 assert_eq!(result.futures_fee_quote_e12,decimal_to_atoms("0.04").unwrap() as i128);
 assert!(result.every_original_fill_price_qty_fee_time_matches
    &&result.spot_history_pagination_exhausted
    &&result.futures_bounded_history_page_nonfull
    &&result.never_reuse_for_order_authorization);
 assert!(!result.global_trade_history_complete
    &&!result.income_funding_transfer_history_complete
    &&!result.current_account_atomicity_proven
    &&!result.no_delayed_fills_after_last_read_proven);
 assert!(!result.may_send_order&&!result.may_auto_unwind&&!result.may_verify_closed);
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),previous);
 assert!(store.load().unwrap().unwrap().active.is_none());
 assert_eq!(api.base.inner.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2c_any_changed_traded_fee_price_quantity_order_id_bill_pagination_denies(){
 use spotarb_native_market_core::native_supervised_recovery_fills::
   audit_fresh_supervised_recovery_fills_once;
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 for (name,mode) in [
   ("spot-different-fee",1),("spot-price",2),("spot-quantity",3),
   ("spot-time",4),("spot-duplicate",5),("spot-foreign",6),
   ("spot-extra-page",7),("spot-no-fee",8),
   ("futures-commission",9),("futures-missing-trade",10),
   ("futures-duplicate-id",11),("futures-foreign-order",12),
   ("futures-direction",13),("futures-fee-asset",14),
   ("future-changed-between-scans",15),("exchange-403",16),
   ("futures-bad-json",17),("spot-changed-second-scan",18),
 ]{
    let (dir,store,exec,gen)=trade1_quarantine_c6();
    let api=Trade2CFillGet::new(exec,mode);
    let at=spotarb_native_market_core::private_https::wall_ms();
    let previous=std::fs::read(dir.join("state.json")).unwrap();
    let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    let outcome=audit_fresh_supervised_recovery_fills_once(
        &api,&store,gen,&trade1_market(at),decimal_to_atoms("20").unwrap(),
        &okx,&bn,at,OkxDomain::Global,10_000);
    assert!(outcome.is_err(),"unexplained fill mutation {name} accepted");
    assert!(api.count()<=50);
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),previous,
        "native state mutated on {name}");
    assert_eq!(api.base.inner.mutations.load(Ordering::SeqCst),0);
    std::fs::remove_dir_all(dir).unwrap();
 }
}


#[test]
fn trade2c_no_get_when_stale_generation_missing_c5_c6_or_expired_bracket(){
 use spotarb_native_market_core::native_supervised_recovery_fills::
    audit_fresh_supervised_recovery_fills_once;
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let api=Trade2CFillGet::new(exec,0);
 let now=spotarb_native_market_core::private_https::wall_ms();
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let m=trade1_market(now);
 assert!(audit_fresh_supervised_recovery_fills_once(
    &api,&store,gen-1,&m,decimal_to_atoms("20").unwrap(),
    &okx,&bn,now,OkxDomain::Global,10_000).is_err());
 assert!(audit_fresh_supervised_recovery_fills_once(
    &api,&store,gen,&m,decimal_to_atoms("20").unwrap(),
    &okx,&bn,now+11_000,OkxDomain::Global,10_000).is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,exec,gen)=micro4_seed("trade2c-missing-c5c6");
 let api=Trade2CFillGet::new(exec,0);
 assert!(audit_fresh_supervised_recovery_fills_once(
    &api,&store,gen,&m,decimal_to_atoms("20").unwrap(),
    &okx,&bn,now,OkxDomain::Global,10_000).is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2c_fresh_pending_order_after_both_fill_scans_invalidates_final_review(){
 use spotarb_native_market_core::native_supervised_recovery_fills::
   audit_fresh_supervised_recovery_fills_once;
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 struct LatePending {
   api:Trade2CFillGet,
   original_binance_order_reads:AtomicUsize,
 }
 impl HttpExecutor for LatePending {
   fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
     let u=url::Url::parse(&req.url).unwrap();
     if u.path()=="/fapi/v1/order"{
       self.original_binance_order_reads.fetch_add(1,Ordering::SeqCst);
     }
     if u.path()=="/fapi/v1/openOrders"
       &&self.original_binance_order_reads.load(Ordering::SeqCst)>=3{
       return Ok(HttpResponse{status:200,headers:vec![],
         body:r#"[{"symbol":"BNTUSDT","orderId":889,"clientOrderId":"late","side":"BUY","positionSide":"BOTH","status":"NEW","reduceOnly":true}]"#.into()});
     }
     self.api.execute_request(req)
   }
 }
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let api=LatePending{api:Trade2CFillGet::new(exec,0),
     original_binance_order_reads:AtomicUsize::new(0)};
 let t=spotarb_native_market_core::private_https::wall_ms();
 let bytes=std::fs::read(dir.join("state.json")).unwrap();
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 assert!(audit_fresh_supervised_recovery_fills_once(
   &api,&store,gen,&trade1_market(t),decimal_to_atoms("20").unwrap(),
   &okx,&bn,t,OkxDomain::Global,10_000).is_err());
 assert_eq!(api.original_binance_order_reads.load(Ordering::SeqCst),3);
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
 assert_eq!(api.api.base.inner.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2c_owner_generation_change_during_per_trade_query_forces_abort(){
 use spotarb_native_market_core::native_supervised_recovery_fills::{
   audit_fresh_supervised_recovery_fills_once,Trade2CFillError,
 };
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 struct Concurrent<'a>{
   api:Trade2CFillGet,
   store:&'a NativeStateStore,gen:u64,advances:AtomicUsize,
 }
 impl HttpExecutor for Concurrent<'_>{
   fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
     let u=url::Url::parse(&req.url).unwrap();
     if u.path()=="/fapi/v1/userTrades"
       &&self.advances.fetch_add(1,Ordering::SeqCst)==0{
       let (new,_)=self.store.transact(self.gen,|_|Ok(())).unwrap();
       assert_eq!(new.generation,self.gen+1);
     }
     self.api.execute_request(req)
   }
 }
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let api=Concurrent{api:Trade2CFillGet::new(exec,0),store:&store,
     gen,advances:AtomicUsize::new(0)};
 let t=spotarb_native_market_core::private_https::wall_ms();
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let result=audit_fresh_supervised_recovery_fills_once(
   &api,&store,gen,&trade1_market(t),decimal_to_atoms("20").unwrap(),
   &okx,&bn,t,OkxDomain::Global,10_000);
 assert!(matches!(result,Err(Trade2CFillError::State(_))));
 assert_eq!(store.load().unwrap().unwrap().generation,gen+1);
 assert_eq!(api.api.base.inner.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2D: every close BLUEPRINT is non-executable, with full spot
// inventory (matched 0.4 + unmatched 0.6), not just matched 0.4.
fn trade2d(
    st:&NativeStateStore,http:&Trade2CFillGet,gen:u64,
    market:&spotarb_native_market_core::market_metadata::NativeMarketMetadata,
    now:u64,
)->Result<spotarb_native_market_core::native_supervised_close_plan::RecoveryFullCloseBlueprint,
spotarb_native_market_core::native_supervised_close_plan::ClosePlanError>{
    use spotarb_native_market_core::{
        native_supervised_close_plan::preview_full_inventory_close,
        private_rest::BinanceCredentials,
    };
    let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    preview_full_inventory_close(http,st,gen,market,now,
        decimal_to_atoms("20").unwrap(),decimal_to_atoms("21").unwrap(),
        &okx,&bn,OkxDomain::Global,10_000)
}
#[test]
fn trade2d_full_spot_inventory_1_bnt_and_binance_reduceonly_0_4_would_need_two_legs(){
 use spotarb_native_market_core::native_supervised_close_plan::{
    simulate_close_fill_progress,HypotheticalCloseLeg,HypotheticalFill,
 };
 let (dir,st,exec,gen)=trade1_quarantine_c6();
 let http=Trade2CFillGet::new(exec,0);
 let t=spotarb_native_market_core::private_https::wall_ms();
 let previous=std::fs::read(dir.join("state.json")).unwrap();
 let p=trade2d(&st,&http,gen,&trade1_market(t),t).unwrap();
 assert_eq!(http.count(),50);
 assert_eq!(p.source_native_generation,gen);
 assert_eq!(p.source_identity_sha256.len(),64);
 assert_eq!(p.total_spot_to_sell_e12,decimal_to_atoms("1").unwrap());
 assert_eq!(p.matched_spot_e12,decimal_to_atoms("0.4").unwrap());
 assert_eq!(p.unmatched_spot_to_remove_e12,decimal_to_atoms("0.6").unwrap());
 assert_eq!(p.binance_short_to_buy_reduce_only_e12,decimal_to_atoms("0.4").unwrap());
 assert_eq!(p.estimated_total_spot_sale_usdt_e12,decimal_to_atoms("20").unwrap());
 assert_eq!(p.estimated_binance_cover_usdt_e12,decimal_to_atoms("8.4").unwrap());
 assert!(p.exceeds_ten_usdt_in_one_leg,"not a $10-pilot sized original fixture");
 assert!(p.split_unmatched_spot_leg_feasible);
 assert!(p.operator_must_select_order_sequence&&p.simulated_not_real);
 assert!(p.fresh_position_and_order_pre_send_recheck_required);
 assert!(!p.all_recovery_order_clients_durable_before_send);
 assert!(!p.may_create_signed_order&&!p.may_send_order
    &&!p.may_auto_close&&!p.may_verify_closed);
 let result=simulate_close_fill_progress(&p,&[
    HypotheticalFill{leg:HypotheticalCloseLeg::OkxSellBase,trade_id:"s1".into(),
        quantity_e12:decimal_to_atoms("1").unwrap(),fee_usdt_e12:decimal_to_atoms("0.02").unwrap()},
    HypotheticalFill{leg:HypotheticalCloseLeg::BinanceBuyReduceOnly,trade_id:"f1".into(),
        quantity_e12:decimal_to_atoms("0.4").unwrap(),fee_usdt_e12:decimal_to_atoms("0.01").unwrap()},
 ]).unwrap();
 assert!(result.hypothetical_flat_only);
 assert_eq!(result.spot_left_e12,0);
 assert_eq!(result.short_left_e12,0);
 assert_eq!(result.directional_unhedged_spot_e12,0);
 assert!(!result.actual_exchange_flatness_proven&&!result.may_verify_closed);
 assert_eq!(previous,std::fs::read(dir.join("state.json")).unwrap());
 assert_eq!(http.base.inner.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2d_partial_simulation_dust_double_trade_ids_overfill_are_never_real_close(){
 use spotarb_native_market_core::native_supervised_close_plan::{
    simulate_close_fill_progress,HypotheticalCloseLeg,HypotheticalFill,
 };
 let (dir,st,exec,gen)=trade1_quarantine_c6();
 let http=Trade2CFillGet::new(exec,0);
 let t=spotarb_native_market_core::private_https::wall_ms();
 let p=trade2d(&st,&http,gen,&trade1_market(t),t).unwrap();
 let a=HypotheticalFill{leg:HypotheticalCloseLeg::OkxSellBase,
    trade_id:"s1".into(),quantity_e12:decimal_to_atoms("0.4").unwrap(),fee_usdt_e12:0};
 let b=HypotheticalFill{leg:HypotheticalCloseLeg::BinanceBuyReduceOnly,
    trade_id:"f1".into(),quantity_e12:decimal_to_atoms("0.2").unwrap(),fee_usdt_e12:0};
 let x=simulate_close_fill_progress(&p,&[a.clone(),b.clone()]).unwrap();
 assert_eq!(x.spot_left_e12,decimal_to_atoms("0.6").unwrap());
 assert_eq!(x.short_left_e12,decimal_to_atoms("0.2").unwrap());
 assert_eq!(x.directional_unhedged_spot_e12,decimal_to_atoms("0.4").unwrap() as i128);
 assert!(!x.hypothetical_flat_only&&!x.actual_exchange_flatness_proven);
 assert!(simulate_close_fill_progress(&p,&[a.clone(),a.clone()]).is_err());
 let mut bad=a.clone();bad.quantity_e12=decimal_to_atoms("1.1").unwrap();
 assert!(simulate_close_fill_progress(&p,&[bad]).is_err());
 let mut bad=b.clone();bad.quantity_e12=decimal_to_atoms("0.5").unwrap();
 assert!(simulate_close_fill_progress(&p,&[bad]).is_err());
 let mut bad=b;bad.quantity_e12=0;
 assert!(simulate_close_fill_progress(&p,&[bad]).is_err());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2d_exchange_min_cost_split_minimum_and_wrong_qty_step_block_or_warn(){
 let (dir,st,exec,gen)=trade1_quarantine_c6();
 let http=Trade2CFillGet::new(exec,0);
 let t=spotarb_native_market_core::private_https::wall_ms();
 let mut m=trade1_market(t);
 m.spot_min_cost="15".into();
 let p=trade2d(&st,&http,gen,&m,t).unwrap();
 assert!(!p.split_unmatched_spot_leg_feasible);
 assert!(!p.may_send_order);
 m.spot_min_cost="21".into();
 assert!(trade2d(&st,&http,gen,&m,t).is_err());
 m.spot_min_cost="0".into();m.future_min_cost="9".into();
 assert!(trade2d(&st,&http,gen,&m,t).is_err());
 m.future_min_cost="5".into();m.future_step_base=Some("0.3".into());
 assert!(trade2d(&st,&http,gen,&m,t).is_err());
 m.future_step_base=Some("0.1".into());m.spot_max_amount=Some("0.9".into());
 assert!(trade2d(&st,&http,gen,&m,t).is_err());
 m.spot_max_amount=Some("2000".into());m.spot_tick=Some("0.03".into());
 assert!(trade2d(&st,&http,gen,&m,t).is_err());
 assert_eq!(http.base.inner.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2d_stale_native_generation_and_changed_commission_no_blueprint(){
 let (dir,st,exec,gen)=trade1_quarantine_c6();
 let http=Trade2CFillGet::new(exec,9);
 let t=spotarb_native_market_core::private_https::wall_ms();
 assert!(trade2d(&st,&http,gen-1,&trade1_market(t),t).is_err());
 assert_eq!(http.count(),0);
 assert!(trade2d(&st,&http,gen,&trade1_market(t),t).is_err());
 assert!(http.count()<=50);
 assert_eq!(http.base.inner.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2D2A MOCK ONLY. Never a CLOSE POST, signed close request, exchange
// authorization, or verified flat position.
fn trade2d2_record(
    store:&NativeStateStore,http:&Trade2CFillGet,gen:u64,now:u64,
    spot_id:&str,future_id:&str,
)->Result<(spotarb_native_market_core::authoritative_state::NativeAuthoritativeState,
    spotarb_native_market_core::native_recovery_dryrun_latch::DryRunLatchWrite),
    spotarb_native_market_core::native_supervised_close_plan::ClosePlanError>{
 use spotarb_native_market_core::private_rest::BinanceCredentials;
 let spot=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let future=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 store.record_mock_close_pair(
     http,gen,&trade1_market(now),now,
     decimal_to_atoms("20").unwrap(),decimal_to_atoms("21").unwrap(),
     &spot,&future,OkxDomain::Global,10_000,spot_id,future_id)
}
#[test]
fn trade2d2_persist_both_original_close_ids_unknown_response_restart_denies_replay(){
 use spotarb_native_market_core::native_recovery_dryrun_latch::{
    DryRunClosePhase,DryRunLatchWrite,
 };
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let api=Trade2CFillGet::new(exec,0);
 let t=spotarb_native_market_core::private_https::wall_ms();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let (prepared,status)=trade2d2_record(
    &store,&api,gen,t,"mock-close-spot-1","mock-close-fut-1").unwrap();
 assert_eq!(status,DryRunLatchWrite::Recorded);
 assert_eq!(api.count(),50);
 assert_eq!(prepared.generation,gen+1);
 let proof=prepared.entry.as_ref().unwrap().dryrun_close_latch.as_ref().unwrap();
 assert_eq!(proof.phase(),DryRunClosePhase::PreparedMockOnly);
 assert_eq!(proof.ids(),("mock-close-spot-1","mock-close-fut-1"));
 assert_eq!(proof.amounts(),(
    decimal_to_atoms("1").unwrap(),decimal_to_atoms("0.4").unwrap(),
    decimal_to_atoms("0.6").unwrap()));
 assert!(!proof.can_send()&&!proof.can_retry()&&!proof.is_real_intent());
 assert_ne!(std::fs::read(dir.join("state.json")).unwrap(),before);
 let reboot=NativeStateStore::open(dir.join("state.json")).unwrap();
 assert_eq!(reboot.load_for_recovery().unwrap().unwrap(),prepared);
 let prior_gets=api.count();
 let (again,kind)=trade2d2_record(
    &reboot,&api,gen+1,t,"mock-close-spot-1","mock-close-fut-1").unwrap();
 assert_eq!(kind,DryRunLatchWrite::Idempotent);
 assert_eq!(again,prepared);
 assert_eq!(api.count(),prior_gets,"replay MUST NOT trigger GET or POST");
 assert!(trade2d2_record(&reboot,&api,gen+1,t,
    "mock-close-spot-2","mock-close-fut-1").is_err());
 let (unknown,kind)=reboot.record_mock_close_timeout(
    gen+1,"mock-close-spot-1","mock-close-fut-1").unwrap();
 assert_eq!(kind,DryRunLatchWrite::Recorded);
 assert_eq!(unknown.generation,gen+2);
 let after_restart=NativeStateStore::open(dir.join("state.json")).unwrap();
 assert_eq!(after_restart.load_for_recovery().unwrap().unwrap(),unknown);
 let p=unknown.entry.as_ref().unwrap().dryrun_close_latch.as_ref().unwrap();
 assert_eq!(p.phase(),DryRunClosePhase::UnknownManualOnly);
 assert!(!p.can_send()&&!p.can_retry());
 let (same,kind)=after_restart.record_mock_close_timeout(
    gen+2,"mock-close-spot-1","mock-close-fut-1").unwrap();
 assert_eq!(kind,DryRunLatchWrite::Idempotent);
 assert_eq!(same,unknown);
 assert!(after_restart.record_mock_close_timeout(gen+2,
    "mock-close-spot-NEW","mock-close-fut-1").is_err());
 assert!(trade2d2_record(&after_restart,&api,gen+2,t,
    "mock-close-spot-NEW","mock-close-fut-NEW").is_err());
 assert_eq!(api.count(),prior_gets);
 assert_eq!(api.base.inner.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2d2_existing_mock_seal_cannot_be_erased_forged_or_phase_reverted(){
 use spotarb_native_market_core::authoritative_state::StatePersistFailpoint;
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let api=Trade2CFillGet::new(exec,0);
 let t=spotarb_native_market_core::private_https::wall_ms();
 let (draft,_)=trade2d2_record(&store,&api,gen,t,"spot-rescue","fut-rescue").unwrap();
 let (unknown,_)=store.record_mock_close_timeout(
    draft.generation,"spot-rescue","fut-rescue").unwrap();
 let raw=std::fs::read(dir.join("state.json")).unwrap();
 for variant in ["erase","entry-delete","rollback-phase","spoof-spot","spoof-future",
  "spoof-quantity","spoof-short","spoof-source","enable-send",
  "enable-retry","fake-sent","fake-verified","fake-release"]{
   let mut altered=unknown.clone();
   let entry=altered.entry.as_mut().unwrap();
   match variant{
      "erase"=>entry.dryrun_close_latch=None,
      "entry-delete"=>altered.entry=None,
      _=>{
         let mut x=serde_json::to_value(entry.dryrun_close_latch.as_ref().unwrap()).unwrap();
         match variant{
          "rollback-phase"=>x["phase"]=serde_json::json!("PreparedMockOnly"),
          "spoof-spot"=>x["spot_client_id"]=serde_json::json!("foreign-client"),
          "spoof-future"=>x["future_client_id"]=serde_json::json!("foreign-client"),
          "spoof-quantity"=>x["spot_qty_e12"]=serde_json::json!(5),
          "spoof-short"=>x["short_qty_e12"]=serde_json::json!(5),
          "spoof-source"=>x["source_hash"]=serde_json::json!("garbage"),
          "enable-send"=>x["may_send_order"]=serde_json::json!(true),
          "enable-retry"=>x["may_retry_order"]=serde_json::json!(true),
          "fake-sent"=>x["actual_order_sent"]=serde_json::json!(true),
          "fake-verified"=>x["may_verify_closed"]=serde_json::json!(true),
          "fake-release"=>x["may_release_cooldown"]=serde_json::json!(true),
          _=>unreachable!(),
         }
         entry.dryrun_close_latch=Some(serde_json::from_value(x).unwrap());
      }
   }
   altered.generation=unknown.generation+1;
   assert!(store.replace(unknown.generation,&altered).is_err(),
      "forged durable phase/identity {variant} accepted");
   assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),raw);
 }
 let mut bypass=unknown.clone();
 bypass.entry.as_mut().unwrap().dryrun_close_latch=None;
 assert!(store.persist_with_failpoint(&bypass,StatePersistFailpoint::None).is_err());
 assert!(store.transact(unknown.generation,|state|{
    state.entry.as_mut().unwrap().dryrun_close_latch=None;Ok(())
 }).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),raw);
 assert_eq!(api.base.inner.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2d2_missing_original_source_forged_ids_and_stale_owner_no_http(){
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let api=Trade2CFillGet::new(exec,0);
 let now=spotarb_native_market_core::private_https::wall_ms();
 let disk=std::fs::read(dir.join("state.json")).unwrap();
 assert!(trade2d2_record(&store,&api,gen-1,now,"spot-a","fut-a").is_err());
 assert!(trade2d2_record(&store,&api,gen,now,"same","same").is_err());
 assert!(trade2d2_record(&store,&api,gen,now,"bad spot","fut-a").is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),disk);
 assert_eq!(api.base.inner.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,exec,gen)=micro4_seed("trade2d2-missing-c5c6");
 let api=Trade2CFillGet::new(exec,0);
 assert!(trade2d2_record(&store,&api,gen,now,"spot-a","fut-a").is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2d2_failed_temp_write_preserves_original_state_and_can_retry(){
 use spotarb_native_market_core::native_recovery_dryrun_latch::DryRunLatchWrite;
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let api=Trade2CFillGet::new(exec,0);
 let t=spotarb_native_market_core::private_https::wall_ms();
 let old=std::fs::read(dir.join("state.json")).unwrap();
 let blocker=dir.join("state.tmp");
 std::fs::create_dir(&blocker).unwrap();
 assert!(trade2d2_record(&store,&api,gen,t,"sim-spot-a","sim-future-a").is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),old);
 std::fs::remove_dir(&blocker).unwrap();
 let (saved,status)=trade2d2_record(
    &store,&api,gen,t,"sim-spot-a","sim-future-a").unwrap();
 assert_eq!(status,DryRunLatchWrite::Recorded);
 assert_eq!(saved.generation,gen+1);
 assert!(saved.entry.unwrap().dryrun_close_latch.is_some());
 assert_eq!(api.base.inner.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2d2_two_owner_writers_cannot_install_two_simulated_id_pairs(){
 use spotarb_native_market_core::native_recovery_dryrun_latch::DryRunLatchWrite;
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let api=Trade2CFillGet::new(exec,0);
 let t=spotarb_native_market_core::private_https::wall_ms();
 let two=std::thread::scope(|scope|{
  let a=scope.spawn(||trade2d2_record(&store,&api,gen,t,"spot-A","future-A"));
  let b=scope.spawn(||trade2d2_record(&store,&api,gen,t,"spot-B","future-B"));
  [a.join().unwrap(),b.join().unwrap()]
 });
 assert_eq!(two.iter().filter(|r|
   matches!(r,Ok((_,DryRunLatchWrite::Recorded)))).count(),1);
 assert_eq!(two.iter().filter(|r|r.is_err()).count(),1);
 let s=store.load().unwrap().unwrap();
 assert_eq!(s.generation,gen+1);
 let pair=s.entry.unwrap().dryrun_close_latch.unwrap();
 assert!(pair.ids()==("spot-A","future-A")||pair.ids()==("spot-B","future-B"));
 assert!(!pair.can_send()&&!pair.can_retry());
 assert_eq!(api.base.inner.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2D2B: simulated private signed GET ONLY. No POST, DELETE or real order.
struct Trade2D2BOrderGet{
    mode:usize,now:u64,calls:AtomicUsize,
}
impl Trade2D2BOrderGet{
    fn new(mode:usize,now:u64)->Self{
        Self{mode,now,calls:AtomicUsize::new(0)}
    }
    fn count(&self)->usize{self.calls.load(Ordering::SeqCst)}
}
impl HttpExecutor for Trade2D2BOrderGet{
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
    assert_eq!(req.method,HttpMethod::Get);
    assert!(!req.mutation&&req.body.is_none());
    let u=url::Url::parse(&req.url).unwrap();
    let path=u.path();
    let n=self.calls.fetch_add(1,Ordering::SeqCst);
    let okx=path=="/api/v5/trade/order";
    assert!(okx||path=="/fapi/v1/order");
    let (mut status,mut qty)=match self.mode{
        1=>("PARTIALLY_FILLED","0.2"),
        2=>("CANCELED","0"),
        _=>("FILLED",if okx{"1"}else{"0.4"}),
    };
    if self.mode==1&&okx{status="partially_filled";qty="0.6";}
    if self.mode==1&&!okx{status="CANCELED";qty="0.2";}
    if self.mode==2&&okx{status="live";qty="0";}
    if self.mode==2&&!okx{status="CANCELED";qty="0";}
    if self.mode==3&&okx{qty="1.1";}
    if self.mode==4&&okx{status="canceled";qty="1";}
    if self.mode==5&&n>=2&&okx{status="partially_filled";qty="0.6";}
    if self.mode==6&&okx{return Ok(HttpResponse{status:404,headers:vec![],
        body:r#"{"code":"51603","msg":"Order does not exist"}"#.into()})}
    if self.mode==7&&!okx{status="NEW";qty="0.2";}
    if self.mode==8&&!okx{qty="0.8";}
    if self.mode==9&&okx{status="surprise_unknown";}
    if self.mode==10&&!okx{return Ok(HttpResponse{status:200,headers:vec![],
        body:"not-json".into()})}
    if self.mode==11&&okx{return Ok(HttpResponse{status:200,headers:vec![],
        body:r#"{"code":"0","data":[]}"#.into()})}
    if self.mode==12&&n>=2&&!okx{status="PARTIALLY_FILLED";qty="0.2";}
    let timestamp=if self.mode==13{self.now-120_000}else{self.now};
    let body=if okx {
        format!(r#"{{"code":"0","data":[{{"instId":"BNT-USDT","clOrdId":"mock-spot-x","ordId":"3301","side":"{}","tdMode":"cash","sz":"1","accFillSz":"{}","state":"{}","uTime":"{}"}}]}}"#,
          if self.mode==14{"buy"}else{"sell"},qty,
          if status=="FILLED"{"filled"}else if status=="CANCELED"{"canceled"}else{status},timestamp)
    }else{
        format!(r#"{{"symbol":"BNTUSDT","clientOrderId":"{}","orderId":4402,"side":"BUY","positionSide":"BOTH","reduceOnly":{},"origQty":"0.4","executedQty":"{}","status":"{}","updateTime":{}}}"#,
            if self.mode==15{"unknown-future"}else{"mock-future-x"},
            if self.mode==16{"false"}else{"true"},qty,status,timestamp)
    };
    Ok(HttpResponse{status:200,headers:vec![],body})
 }
}
fn trade2d2b_seed()->(std::path::PathBuf,NativeStateStore,
  spotarb_native_market_core::authoritative_state::NativeAuthoritativeState,u64){
    let (dir,store,base,gen)=trade1_quarantine_c6();
    let api=Trade2CFillGet::new(base,0);
    let now=spotarb_native_market_core::private_https::wall_ms();
    let (prepared,_)=trade2d2_record(&store,&api,gen,now,
        "mock-spot-x","mock-future-x").unwrap();
    let (unknown,_)=store.record_mock_close_timeout(
        prepared.generation,"mock-spot-x","mock-future-x").unwrap();
    (dir,store,unknown,now)
}
fn trade2d2b_inspect(
    store:&NativeStateStore,gen:u64,at:u64,http:&Trade2D2BOrderGet,
)->Result<spotarb_native_market_core::native_dryrun_close_status::MockCloseStatusReview,
  spotarb_native_market_core::native_dryrun_close_status::MockCloseStatusError>{
    use spotarb_native_market_core::{
        private_rest::BinanceCredentials,
        native_dryrun_close_status::inspect_mock_unknown_close,
    };
    let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    inspect_mock_unknown_close(http,store,gen,&okx,&bin,at,OkxDomain::Global,10_000)
}


#[test]
fn trade2d2b_double_get_filled_is_still_only_mock_diagnostic_and_never_verified_closed(){
 use spotarb_native_market_core::native_dryrun_close_status::ObservedCloseState;
 let (dir,store,_,now)=trade2d2b_seed();
 let fresh=NativeStateStore::open(dir.join("state.json")).unwrap();
 let state=fresh.load_for_recovery().unwrap().unwrap();
 let bytes=std::fs::read(dir.join("state.json")).unwrap();
 let api=Trade2D2BOrderGet::new(0,now);
 let output=trade2d2b_inspect(&fresh,state.generation,now,&api).unwrap();
 assert_eq!(api.count(),4);
 assert_eq!(output.signed_get_count,4);
 assert_eq!(output.okx.state,ObservedCloseState::TerminalFilled);
 assert_eq!(output.binance.state,ObservedCloseState::TerminalFilled);
 assert_eq!(output.okx.filled_e12,decimal_to_atoms("1").unwrap());
 assert_eq!(output.binance.filled_e12,decimal_to_atoms("0.4").unwrap());
 assert!(output.identical_double_reads&&output.requires_manual_reconciliation);
 assert!(!output.actual_order_dispatch_proven);
 assert!(!output.original_trades_and_exit_fees_proven);
 assert!(!output.open_orders_and_positions_flat_proven);
 assert!(!output.may_cancel_order&&!output.may_retry_order
       &&!output.may_send_order&&!output.may_verify_closed);
 assert_eq!(fresh.load().unwrap().unwrap(),state);
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2d2b_partial_cancel_and_nonfilled_open_status_need_manual_reconciliation(){
 use spotarb_native_market_core::native_dryrun_close_status::ObservedCloseState;
 for (mode,spot,futures) in [
  (1,ObservedCloseState::OpenPartial,ObservedCloseState::TerminalCanceledPartial),
  (2,ObservedCloseState::OpenNoFill,ObservedCloseState::TerminalCanceledNoFill),
 ]{
    let (dir,store,state,at)=trade2d2b_seed();
    let api=Trade2D2BOrderGet::new(mode,at);
    let report=trade2d2b_inspect(&store,state.generation,at,&api).unwrap();
    assert_eq!(report.okx.state,spot);
    assert_eq!(report.binance.state,futures);
    assert!(!report.may_retry_order&&!report.may_verify_closed);
    assert!(report.requires_manual_reconciliation);
    assert_eq!(api.count(),4);
    std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2d2b_bad_identity_drift_partial_semantics_not_found_or_http_errors_fail_closed(){
 for (name,mode) in [
  ("overfill",3),("cancel-full-ambiguous",4),("changed-second-get",5),
  ("order-not-found",6),("new-nonzero-fill",7),("futures-overfill",8),
  ("unknown-status",9),("malformed-JSON",10),("missing-OKX-order",11),
  ("binance-second-read-variant",12),("stale-update-time",13),
  ("wrong-OKX-side",14),("wrong-futures-client",15),
  ("missing-reduceOnly",16)
 ]{
   let (dir,store,state,at)=trade2d2b_seed();
   let api=Trade2D2BOrderGet::new(mode,at);
   let prior=std::fs::read(dir.join("state.json")).unwrap();
   assert!(trade2d2b_inspect(&store,state.generation,at,&api).is_err(),
      "bad mock order observation {name} accepted");
   assert!(api.count()<=4);
   assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),prior);
   std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2d2b_missing_unknown_latch_and_stale_generation_prevent_even_get(){
 let (dir,store,state,at)=trade2d2b_seed();
 let api=Trade2D2BOrderGet::new(0,at);
 assert!(trade2d2b_inspect(&store,state.generation-1,at,&api).is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,exec,gen)=trade1_quarantine_c6();
 let api=Trade2D2BOrderGet::new(0,at);
 assert!(trade2d2b_inspect(&store,gen,at,&api).is_err());
 assert_eq!(api.count(),0);
 assert_eq!(exec.mutations.load(Ordering::SeqCst),0);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2d2b_native_owner_changes_during_last_get_result_is_discarded(){
 let (dir,store,s,now)=trade2d2b_seed();
 struct RacingStatus<'a>{
    base:Trade2D2BOrderGet,store:&'a NativeStateStore,
    generation:u64,modified:AtomicUsize,
 }
 impl HttpExecutor for RacingStatus<'_>{
    fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
        if self.base.count()==3&&self.modified.fetch_add(1,Ordering::SeqCst)==0{
            let (n,_)=self.store.transact(self.generation,|_|Ok(())).unwrap();
            assert_eq!(n.generation,self.generation+1);
        }
        self.base.execute_request(req)
    }
 }
 let http=RacingStatus{
    base:Trade2D2BOrderGet::new(0,now),
    store:&store,generation:s.generation,modified:AtomicUsize::new(0),
 };
 use spotarb_native_market_core::{
   native_dryrun_close_status::inspect_mock_unknown_close,
   private_rest::BinanceCredentials,
 };
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 assert!(inspect_mock_unknown_close(&http,&store,s.generation,&okx,&bn,
    now,OkxDomain::Global,10_000).is_err());
 assert_eq!(http.base.count(),4);
 assert_eq!(store.load().unwrap().unwrap().generation,s.generation+1);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2D2C1 only: order statuses BEFORE and AFTER two independent
// CLOSE per-trade history scans, all fake signed GET. Must NEVER call POST.
struct Trade2D2CFillsGet {
    order:Trade2D2BOrderGet,
    mode:usize,
    total:AtomicUsize,
    okx_first:AtomicUsize,
    bin_first:AtomicUsize,
}
impl Trade2D2CFillsGet{
    fn new(mode:usize,now:u64)->Self{
        Self{order:Trade2D2BOrderGet::new(if mode==15{1}else{0},now),
             mode,total:AtomicUsize::new(0),
             okx_first:AtomicUsize::new(0),bin_first:AtomicUsize::new(0)}
    }
    fn count(&self)->usize{self.total.load(Ordering::SeqCst)}
}
impl HttpExecutor for Trade2D2CFillsGet{
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
    assert_eq!(req.method,HttpMethod::Get);
    assert!(!req.mutation&&req.body.is_none());
    self.total.fetch_add(1,Ordering::SeqCst);
    let u=url::Url::parse(&req.url).unwrap();
    let path=u.path();
    if path=="/api/v5/trade/order"||path=="/fapi/v1/order"{
        return self.order.execute_request(req)
    }
    let now=self.order.now;
    let params=u.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
    let okx=path=="/api/v5/trade/fills-history";
    let bin=path=="/fapi/v1/userTrades";
    assert!(okx||bin,"unexpected mock close route {path}");
    let tail=if okx{params.contains_key("after")}
        else{params.get("fromId").map(|v|v.as_ref()!="0").unwrap_or(false)};
    let first_reads=if okx {
        if tail{self.okx_first.load(Ordering::SeqCst)}
        else{self.okx_first.fetch_add(1,Ordering::SeqCst)}
    }else{
        if tail{self.bin_first.load(Ordering::SeqCst)}
        else{self.bin_first.fetch_add(1,Ordering::SeqCst)}
    };
    if self.mode==13&&bin&&!tail{
        return Ok(HttpResponse{status:403,headers:vec![],body:"{}".into()})
    }
    let response=if tail{
        if self.mode==8&&bin{
            serde_json::json!([{"symbol":"BNTUSDT","id":8003,"orderId":4402,
                "side":"BUY","positionSide":"BOTH","marginAsset":"USDT",
                "buyer":true,"realizedPnl":"0","price":"21",
                "qty":"0.01","commission":"0.001","commissionAsset":"USDT",
                "time":now-20}])
        }else if self.mode==9&&okx{
            serde_json::json!({"code":"0","data":[{"instId":"BNT-USDT",
              "ordId":"3301","billId":"900","tradeId":"additional",
              "fillPx":"20","fillSz":"0.01","fee":"-0.001",
              "feeCcy":"USDT","fillTime":now-20}]})
        }else if okx{serde_json::json!({"code":"0","data":[]})}
        else{serde_json::json!([])}
    }else if okx{
        let mut row=serde_json::json!({"ordId":"3301","billId":"901",
             "tradeId":"exit-spot-1","fillPx":"20","fillSz":"1",
             "fee":"-0.04","feeCcy":"USDT","fillTime":now-25});
        match self.mode{
          1 if first_reads>=1=>row["fee"]=serde_json::json!("-0.09"),
          4=>row["fillSz"]=serde_json::json!("0.9"),
          10=>row["feeCcy"]=serde_json::json!("BNT"),
          11=>row["fillTime"]=serde_json::Value::Null,
          16 if first_reads>=1=>row["fillPx"]=serde_json::json!("22"),
          _=>{},
        }
        let mut rows=vec![row];
        if self.mode==17{rows.push(rows[0].clone())}
        if self.mode==18{rows[0]["ordId"]=serde_json::json!("3333")}
        serde_json::json!({"code":"0","data":rows})
    }else{
        let mut row1=serde_json::json!({"symbol":"BNTUSDT","id":8001,
           "orderId":4402,"side":"BUY","positionSide":"BOTH",
           "marginAsset":"USDT","buyer":true,"realizedPnl":"-0.03",
           "price":"21","qty":"0.2","commission":"0.005",
           "commissionAsset":"USDT","time":now-26});
        let mut row2=serde_json::json!({"symbol":"BNTUSDT","id":8002,
           "orderId":4402,"side":"BUY","positionSide":"BOTH",
           "marginAsset":"USDT","buyer":true,"realizedPnl":"-0.03",
           "price":"21","qty":"0.2","commission":"0.005",
           "commissionAsset":"USDT","time":now-24});
        match self.mode{
          2 if first_reads>=1=>row2["commission"]=serde_json::json!("0.05"),
          3=>row2["id"]=serde_json::json!(8001),
          5=>row2["qty"]=serde_json::json!("0.1"),
          6=>row1["side"]=serde_json::json!("SELL"),
          7=>row1["commissionAsset"]=serde_json::json!("BNB"),
          12=>row1["realizedPnl"]=serde_json::json!("broken"),
          14=>row1["price"]=serde_json::json!("0"),
          19=>row2["orderId"]=serde_json::json!(4403),
          20=>row2["time"]=serde_json::Value::Null,
          21 if first_reads>=1=>row2["realizedPnl"]=serde_json::json!("-0.04"),
          _=>{},
        }
        serde_json::json!([row1,row2])
    };
    Ok(HttpResponse{status:200,headers:vec![],body:response.to_string()})
 }
}
fn trade2d2c_check(
 store:&NativeStateStore,g:u64,now:u64,api:&Trade2D2CFillsGet,
)->Result<spotarb_native_market_core::native_dryrun_close_fills::MockCloseTradeReceipts,
spotarb_native_market_core::native_dryrun_close_fills::MockCloseFillsError>{
 use spotarb_native_market_core::{
   native_dryrun_close_fills::inspect_mock_full_close_trades,
   private_rest::BinanceCredentials,
 };
 let spot=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 inspect_mock_full_close_trades(api,store,g,&spot,&bn,now,OkxDomain::Global,10_000)
}


#[test]
fn trade2d2c_double_full_close_trade_ids_fees_and_realized_pnl_do_not_verify_flatness(){
 let (dir,store,state,at)=trade2d2b_seed();
 let api=Trade2D2CFillsGet::new(0,at);
 let old=std::fs::read(dir.join("state.json")).unwrap();
 let report=trade2d2c_check(&store,state.generation,at,&api).unwrap();
 assert_eq!(api.count(),16);
 assert_eq!(report.total_readonly_gets,16);
 assert_eq!(report.original_spot_trade_ids,vec!["exit-spot-1".to_string()]);
 assert_eq!(report.original_futures_trade_ids,
    vec!["8001".to_string(),"8002".to_string()]);
 assert_eq!(report.spot_fee_usdt_e12,decimal_to_atoms("0.04").unwrap() as i128);
 assert_eq!(report.futures_fee_usdt_e12,decimal_to_atoms("0.01").unwrap() as i128);
 assert_eq!(report.futures_reported_close_realized_pnl_usdt_e12,
    -(decimal_to_atoms("0.06").unwrap() as i128));
 assert!(report.complete_bounded_order_fill_scan_twice
    &&report.source_is_simulated_not_actual_order_intent);
 assert!(!report.current_futures_position_flat_proven
    &&!report.spot_base_balance_flat_proven
    &&!report.all_outstanding_and_trigger_orders_exhausted
    &&!report.account_wide_funding_and_transfers_complete
    &&!report.lifetime_net_pnl_proven
    &&!report.no_fills_after_last_get_proven);
 assert!(!report.may_send_or_retry_order&&!report.may_verify_closed);
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),old);
 assert!(store.load().unwrap().unwrap().active.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2d2c_per_fill_fee_price_identity_time_duplicate_and_tail_changes_fail_closed(){
 for (label,mode) in [
    ("OKX-second-scan-fee",1),("Binance-second-scan-fee",2),
    ("Binance-duplicate-trade-ID",3),("OKX-quantity",4),
    ("Binance-quantity",5),("Binance-wrong-side",6),
    ("Binance-fee-not-USDT",7),("Binance-tail-not-empty",8),
    ("OKX-tail-not-empty",9),("OKX-fee-not-USDT",10),
    ("OKX-missing-trade-time",11),("Binance-invalid-pnl",12),
    ("exchange-403",13),("Binance-zero-price",14),
    ("not-fully-filled",15),("OKX-price-drift",16),
    ("OKX-duplicate-bill-trade",17),("OKX-foreign-order",18),
    ("Binance-foreign-order",19),("Binance-missing-time",20),
    ("Binance-second-pnl-drift",21)
 ]{
    let (dir,store,state,at)=trade2d2b_seed();
    let api=Trade2D2CFillsGet::new(mode,at);
    let original=std::fs::read(dir.join("state.json")).unwrap();
    assert!(trade2d2c_check(&store,state.generation,at,&api).is_err(),
       "unsafe changed close trade/fees somehow accepted {label}");
    assert!(api.count()<=16);
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
    std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2d2c_without_mock_unknown_or_with_stale_owner_no_history_queries(){
 let (dir,store,state,at)=trade2d2b_seed();
 let api=Trade2D2CFillsGet::new(0,at);
 assert!(trade2d2c_check(&store,state.generation-1,at,&api).is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_http,gen)=trade1_quarantine_c6();
 let api=Trade2D2CFillsGet::new(0,at);
 assert!(trade2d2c_check(&store,gen,at,&api).is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
}


struct Trade2D2CFlatGet{
    original:Trade2D2CFillsGet,
    mode:usize,now:u64,baseline:String,
    count:AtomicUsize,
    wallet_reads:AtomicUsize,
    okx_balance_reads:AtomicUsize,
}
impl Trade2D2CFlatGet{
    fn new(mode:usize,now:u64,baseline:String)->Self{
        Self{original:Trade2D2CFillsGet::new(0,now),
            mode,now,baseline,count:AtomicUsize::new(0),
            wallet_reads:AtomicUsize::new(0),
            okx_balance_reads:AtomicUsize::new(0)}
    }
    fn count(&self)->usize{self.count.load(Ordering::SeqCst)}
}
impl HttpExecutor for Trade2D2CFlatGet{
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
    assert_eq!(req.method,HttpMethod::Get);
    assert!(!req.mutation&&req.body.is_none());
    self.count.fetch_add(1,Ordering::SeqCst);
    let u=url::Url::parse(&req.url).unwrap();
    let path=u.path();
    let now=if self.mode==12{self.now-15_000}else{self.now};
    let body=match path{
       "/fapi/v1/openOrders"=>
          if self.mode==1||self.mode==16{
             serde_json::json!([{"symbol":"BNTUSDT","orderId":501,"clientOrderId":"late",
                  "side":"BUY","positionSide":"BOTH","status":"NEW","reduceOnly":true}])
          }else{serde_json::json!([])},
       "/fapi/v3/positionRisk"=>
          if self.mode==14{serde_json::json!([])}
          else{serde_json::json!([
           {"symbol":"BNTUSDT","positionAmt":
              if self.mode==2{"-0.01"}else{"0"},
            "positionSide":"BOTH","marginAsset":"USDT","updateTime":now},
           {"symbol":"ZECUSDT","positionAmt":
              if self.mode==3{"-0.02"}else{"0"},
            "positionSide":"BOTH","marginAsset":"USDT","updateTime":now}])},
       "/fapi/v3/balance"=>{
          let nth=self.wallet_reads.fetch_add(1,Ordering::SeqCst);
          let amount=if self.mode==4||(self.mode==5&&nth>=1){"99.90"}
              else{"99.93"};
          serde_json::json!([{"asset":"USDT","balance":amount,
              "availableBalance":if self.mode==6{"99.5"}else{amount},
              "updateTime":now}])
       }
       "/api/v5/trade/orders-pending"=>
          if self.mode==7{
              serde_json::json!({"code":"0","data":[
                 {"instId":"BNT-USDT","ordId":"12","clOrdId":"late",
                  "state":"live","side":"sell","sz":"0.1","accFillSz":"0"}]})
          }else{serde_json::json!({"code":"0","data":[]})},
       "/api/v5/account/positions"=>
          if self.mode==8{serde_json::json!({"code":"0","data":[
             {"instType":"SWAP","instId":"BTC-USDT-SWAP",
              "pos":"0.01","posSide":"net"}]})}
          else{serde_json::json!({"code":"0","data":[]})},
       "/api/v5/account/balance"=>{
          let nth=self.okx_balance_reads.fetch_add(1,Ordering::SeqCst);
          let mut x=self.baseline.clone();
          if self.mode==9||(self.mode==11&&nth>=1){x="0.9".into()}
          let free=if self.mode==10{"0".to_string()}else{x.clone()};
          serde_json::json!({"code":"0","data":[{"details":[
             {"ccy":"BNT","cashBal":x,"availBal":free,
              "frozenBal":if self.mode==10{"0.1"}else{"0"},
              "uTime":now}]}]})
       }
       _=>return self.original.execute_request(req)
    };
    if self.mode==13&&path=="/fapi/v3/positionRisk"{
        return Ok(HttpResponse{status:403,headers:vec![],body:"{}".into()})
    }
    Ok(HttpResponse{status:200,headers:vec![],body:body.to_string()})
 }
}
fn trade2d2c_flat(
 store:&NativeStateStore,g:u64,at:u64,http:&Trade2D2CFlatGet,
)->Result<spotarb_native_market_core::native_dryrun_close_accounts::MockFinalAccountReview,
spotarb_native_market_core::native_dryrun_close_accounts::MockFinalAccountError>{
 use spotarb_native_market_core::{
  native_dryrun_close_accounts::inspect_mock_full_close_accounts,
  private_rest::BinanceCredentials,
 };
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKESECRET").unwrap();
 inspect_mock_full_close_accounts(http,store,g,&okx,&bin,at,OkxDomain::Global,10_000)
}
fn trade2d2c_baseline(
 s:&spotarb_native_market_core::authoritative_state::NativeAuthoritativeState
)->String{
 use spotarb_native_market_core::fill_economics::atoms_to_string;
 let e=s.entry.as_ref().unwrap();
 assert_eq!(e.plan.spot_baseline_atoms_e18%1_000_000,0);
 atoms_to_string(e.plan.spot_baseline_atoms_e18/1_000_000)
}


#[test]
fn trade2d2c_two_independent_final_account_brackets_still_no_verified_close_or_lifetime_profit(){
 let (dir,store,state,at)=trade2d2b_seed();
 let api=Trade2D2CFlatGet::new(0,at,trade2d2c_baseline(&state));
 let prior=std::fs::read(dir.join("state.json")).unwrap();
 let x=trade2d2c_flat(&store,state.generation,at,&api).unwrap();
 assert_eq!(api.count(),28);
 assert_eq!(x.total_signed_gets,28);
 assert_eq!(x.independent_post_close_account_brackets,2);
 assert!(!x.distinct_exchange_update_epochs_proven,
   "two scripted samples cannot attest distinct exchange-side updates");
 assert_eq!(x.observed_okx_base_available_e12,
    decimal_to_atoms(&trade2d2c_baseline(&state)).unwrap());
 assert_eq!(x.observed_binance_usdt_wallet_e12,decimal_to_atoms("99.93").unwrap());
 assert!(x.synthetic_regular_open_orders_zero
   &&x.synthetic_futures_and_okx_derivatives_positions_zero
   &&x.synthetic_okx_base_restored_to_pre_entry_e12
   &&x.synthetic_binance_wallet_delta_matches_close_fees_and_pnl);
 assert_eq!(x.close_trades.total_readonly_gets,16);
 assert!(!x.actual_signed_live_close_proven
    &&!x.all_conditional_and_algo_orders_exhausted
    &&!x.historical_funding_and_transfers_exhausted
    &&!x.whole_cycle_realized_net_pnl_proven
    &&!x.no_post_observation_delayed_execution_proven
    &&!x.independent_external_antirollback_proven
    &&!x.may_retry_or_cancel&&!x.may_verify_closed);
 assert_eq!(prior,std::fs::read(dir.join("state.json")).unwrap());
 assert_eq!(store.load().unwrap().unwrap(),state);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2d2c_stale_foreign_or_changed_final_wallet_positions_open_orders_denied(){
 for (reason,mode) in [
   ("new-binance-order",1),("still-original-short",2),("foreign-short",3),
   ("wallet-adjustment-unknown",4),("wallet-drifts-between-brackets",5),
   ("wallet-not-entirely-available",6),("okx-late-open-spot-order",7),
   ("okx-nonflat-derivative",8),("okx-unmatched-BNT",9),
   ("okx-frozen-base-asset",10),("okx-base-change-on-second-read",11),
   ("stale-exchange-timestamp",12),("private-HTTP-403",13),
   ("missing-Binance-original-position",14),("foreign-signed-order",16),
 ]{
   let (dir,store,state,at)=trade2d2b_seed();
   let api=Trade2D2CFlatGet::new(mode,at,trade2d2c_baseline(&state));
   let old=std::fs::read(dir.join("state.json")).unwrap();
   assert!(trade2d2c_flat(&store,state.generation,at,&api).is_err(),
     "unverified flat account accepted {reason}");
   assert!(api.count()<=28);
   assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),old);
   std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2d2c_flat_no_unknown_latch_or_stale_owner_makes_zero_gets(){
 let (dir,store,state,at)=trade2d2b_seed();
 let api=Trade2D2CFlatGet::new(0,at,trade2d2c_baseline(&state));
 assert!(trade2d2c_flat(&store,state.generation-1,at,&api).is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_exec,gen)=trade1_quarantine_c6();
 let api=Trade2D2CFlatGet::new(0,at,"0".into());
 assert!(trade2d2c_flat(&store,gen,at,&api).is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2d2c_native_owner_replaced_during_second_final_account_bracket_denies(){
 use spotarb_native_market_core::{
  native_dryrun_close_accounts::inspect_mock_full_close_accounts,
  private_rest::BinanceCredentials,
 };
 let (dir,store,state,at)=trade2d2b_seed();
 struct Concurrent<'a>{
    base:Trade2D2CFlatGet,
    store:&'a NativeStateStore,gen:u64,
    final_binance_open_orders:AtomicUsize,
    changed:AtomicUsize,
 }
 impl HttpExecutor for Concurrent<'_>{
    fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
        let url=url::Url::parse(&req.url).unwrap();
        if url.path()=="/fapi/v1/openOrders"{
          let n=self.final_binance_open_orders.fetch_add(1,Ordering::SeqCst);
          if n==1&&self.changed.fetch_add(1,Ordering::SeqCst)==0{
              let (new,_)=self.store.transact(self.gen,|_|Ok(())).unwrap();
              assert_eq!(new.generation,self.gen+1);
          }
        }
        self.base.execute_request(req)
    }
 }
 let api=Concurrent{
   base:Trade2D2CFlatGet::new(0,at,trade2d2c_baseline(&state)),
   store:&store,gen:state.generation,
   final_binance_open_orders:AtomicUsize::new(0),
   changed:AtomicUsize::new(0),
 };
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKESECRET").unwrap();
 let result=inspect_mock_full_close_accounts(&api,&store,state.generation,
   &okx,&bn,at,OkxDomain::Global,10_000);
 assert!(result.is_err(),"racing owner must invalidate account flatness");
 assert_eq!(api.base.count(),28);
 assert_eq!(api.changed.load(Ordering::SeqCst),1);
 assert_eq!(store.load().unwrap().unwrap().generation,state.generation+1);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2D2D all-account pending algo coverage. No real network; 28
// original mock closing history/account GETs + 2 x 9 algo GETs.
struct Trade2D2DAlgoGet{
    account:Trade2D2CFlatGet,
    mode:usize,
    count:AtomicUsize,
    binance_algo_reads:AtomicUsize,
    okx_algo_reads:AtomicUsize,
}
impl Trade2D2DAlgoGet{
 fn new(mode:usize,now:u64,baseline:String)->Self{
    Self{account:Trade2D2CFlatGet::new(0,now,baseline),mode,
      count:AtomicUsize::new(0),binance_algo_reads:AtomicUsize::new(0),
      okx_algo_reads:AtomicUsize::new(0)}
 }
 fn total(&self)->usize{self.count.load(Ordering::SeqCst)}
}
impl HttpExecutor for Trade2D2DAlgoGet{
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
    assert_eq!(req.method,HttpMethod::Get);
    assert!(!req.mutation&&req.body.is_none());
    self.count.fetch_add(1,Ordering::SeqCst);
    let url=url::Url::parse(&req.url).unwrap();
    let path=url.path();
    let bn=path=="/fapi/v1/openAlgoOrders";
    let ok=path=="/api/v5/trade/orders-algo-pending";
    if !bn&&!ok{return self.account.execute_request(req)}
    let q=url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
    let n=if bn{
       assert!(!q.contains_key("symbol")&&!q.contains_key("algoType"),
              "MUST scan ALL Binance account symbols and algo families");
       self.binance_algo_reads.fetch_add(1,Ordering::SeqCst)
    }else{
       assert!(!q.contains_key("symbol")&&!q.contains_key("instId")
           &&!q.contains_key("instType"),
           "MUST scan ALL OKX account symbols, not only BNT");
       assert_eq!(q.get("limit").map(|s|s.as_ref()),Some("100"));
       let kind=q.get("ordType").unwrap().as_ref();
       assert!(spotarb_native_market_core::native_dryrun_pending_algos::
           OKX_PENDING_ALGO_TYPES.contains(&kind));
       self.okx_algo_reads.fetch_add(1,Ordering::SeqCst)
    };
    if self.mode==3&&ok{
       return Ok(HttpResponse{status:200,headers:vec![],
          body:r#"{"code":"51000","data":[]}"#.into()})
    }
    if self.mode==4&&bn{
       return Ok(HttpResponse{status:403,headers:vec![],
          body:r#"{"code":-2015}"#.into()})
    }
    if self.mode==5&&ok{
       return Ok(HttpResponse{status:200,headers:vec![],
          body:r#"{"code":"0","data":{}}"#.into()})
    }
    if self.mode==6&&bn{
       return Ok(HttpResponse{status:200,headers:vec![],
          body:r#"{"orders":[]}"#.into()})
    }
    let bad_binance=self.mode==1||(self.mode==7&&n>=1);
    let okx_kind=q.get("ordType").map(|s|s.as_ref()).unwrap_or("");
    let bad_okx=self.mode==2 && okx_kind=="oco"
      ||self.mode==8&&okx_kind=="smart_iceberg"
      ||self.mode==9&&n>=8 && okx_kind=="conditional";
    let body=if bn{
       if bad_binance{
          r#"[{"algoId":55,"symbol":"ZECUSDT","algoType":"CONDITIONAL",
              "algoStatus":"NEW","clientAlgoId":"foreign"}]"#.to_string()
       }else{"[]".to_string()}
    }else if bad_okx{
       r#"{"code":"0","data":[{"algoId":"112","instId":"ZEC-USDT",
          "ordType":"oco","state":"live"}]}"#.to_string()
    }else{
       r#"{"code":"0","data":[]}"#.to_string()
    };
    Ok(HttpResponse{status:200,headers:vec![],body})
 }
}
fn trade2d2d_run(
    store:&NativeStateStore,gen:u64,at:u64,http:&Trade2D2DAlgoGet,
)->Result<spotarb_native_market_core::native_dryrun_pending_algos::MockPendingAlgoAudit,
    spotarb_native_market_core::native_dryrun_pending_algos::MockAlgoError>{
 use spotarb_native_market_core::{
    native_dryrun_pending_algos::inspect_mock_pending_algos_after_close,
    private_rest::BinanceCredentials,
 };
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 inspect_mock_pending_algos_after_close(http,store,gen,&okx,&bin,at,OkxDomain::Global,10_000)
}


#[test]
fn trade2d2d_full_account_algo_survey_is_46_mock_get_never_a_real_close_permit(){
 let (dir,store,state,at)=trade2d2b_seed();
 let http=Trade2D2DAlgoGet::new(0,at,trade2d2c_baseline(&state));
 let prior=std::fs::read(dir.join("state.json")).unwrap();
 let result=trade2d2d_run(&store,state.generation,at,&http).unwrap();
 assert_eq!(http.total(),46);
 assert_eq!(http.binance_algo_reads.load(Ordering::SeqCst),2);
 assert_eq!(http.okx_algo_reads.load(Ordering::SeqCst),16);
 assert_eq!(result.total_mock_signed_gets,46);
 assert_eq!(result.independent_algo_snapshots,2);
 assert!(result.binance_all_symbols_open_algo_zero_in_both);
 assert!(result.okx_all_symbols_known_algo_types_zero_in_both);
 assert_eq!(result.checked_okx_types.len(),8);
 assert_eq!(result.checked_okx_types,
    spotarb_native_market_core::native_dryrun_pending_algos::
    OKX_PENDING_ALGO_TYPES.iter().map(|x|x.to_string()).collect::<Vec<_>>());
 assert!(!result.no_new_types_or_endpoint_semantics_proven
  &&!result.no_future_delayed_trigger_or_cross_venue_race_proven
  &&!result.actual_exchange_order_ever_sent_proven
  &&!result.global_income_and_lifetime_pnl_proven
  &&!result.may_cancel_or_retry_order
  &&!result.may_send_order
  &&!result.may_mark_verified_closed);
 assert_eq!(result.previous.total_signed_gets,28);
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),prior);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2d2d_hidden_binance_or_okx_foreign_algo_pending_or_bad_json_is_hard_deny(){
 for (label,mode) in [
  ("Binance conditional on another symbol",1),
  ("OKX OCO for different instrument",2),
  ("OKX nonzero business error",3),
  ("Binance permission/HTTP error",4),
  ("OKX wrong JSON envelope",5),
  ("Binance wrong JSON envelope",6),
  ("Binance NEW algo between rounds",7),
  ("OKX smart iceberg foreign algo",8),
  ("OKX new conditional on second round",9),
 ]{
    let (dir,store,state,at)=trade2d2b_seed();
    let http=Trade2D2DAlgoGet::new(mode,at,trade2d2c_baseline(&state));
    let raw=std::fs::read(dir.join("state.json")).unwrap();
    assert!(trade2d2d_run(&store,state.generation,at,&http).is_err(),
        "{label} incorrectly accepted as no risk");
    assert!(http.total()<=46);
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),raw);
    std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2d2d_bad_owner_or_missing_unknown_latch_aborts_without_get(){
 let (dir,store,state,at)=trade2d2b_seed();
 let http=Trade2D2DAlgoGet::new(0,at,trade2d2c_baseline(&state));
 assert!(trade2d2d_run(&store,state.generation-1,at,&http).is_err());
 assert_eq!(http.total(),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_source,gen)=trade1_quarantine_c6();
 let http=Trade2D2DAlgoGet::new(0,at,"0".into());
 assert!(trade2d2d_run(&store,gen,at,&http).is_err());
 assert_eq!(http.total(),0);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2d2d_owner_update_on_second_algo_bracket_invalidates_stale_zero_report(){
 use spotarb_native_market_core::{
    native_dryrun_pending_algos::inspect_mock_pending_algos_after_close,
    private_rest::BinanceCredentials,
 };
 let (dir,store,state,at)=trade2d2b_seed();
 struct Mutating<'a>{
    data:Trade2D2DAlgoGet,
    st:&'a NativeStateStore,gen:u64,moved:AtomicUsize,
 }
 impl HttpExecutor for Mutating<'_>{
    fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
        let path=url::Url::parse(&req.url).unwrap();
        if path.path()=="/fapi/v1/openAlgoOrders"
          &&self.data.binance_algo_reads.load(Ordering::SeqCst)==1
          &&self.moved.fetch_add(1,Ordering::SeqCst)==0{
            let (saved,_)=self.st.transact(self.gen,|_|Ok(())).unwrap();
            assert_eq!(saved.generation,self.gen+1);
        }
        self.data.execute_request(req)
    }
 }
 let http=Mutating{
   data:Trade2D2DAlgoGet::new(0,at,trade2d2c_baseline(&state)),
   st:&store,gen:state.generation,moved:AtomicUsize::new(0)
 };
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 assert!(inspect_mock_pending_algos_after_close(
    &http,&store,state.generation,&okx,&bn,at,OkxDomain::Global,10_000).is_err());
 assert_eq!(http.data.total(),46);
 assert_eq!(http.moved.load(Ordering::SeqCst),1);
 assert_eq!(store.load().unwrap().unwrap().generation,state.generation+1);
 assert_eq!(http.data.account.original.order.count(),8);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2D2E1: full previous 46 signed MOCK GETs + historical scans.
// Conservative: ANY historical algo row requires manual child-order
// reconciliation, including CANCELLED and seemingly harmless orders.
struct Trade2D2EHistoryGet{
    prior:Trade2D2DAlgoGet,
    mode:usize,
    total:AtomicUsize,
    binance_history:AtomicUsize,
    okx_history:AtomicUsize,
}
impl Trade2D2EHistoryGet{
 fn new(mode:usize,at:u64,baseline:String)->Self{
    Self{prior:Trade2D2DAlgoGet::new(0,at,baseline),mode,
         total:AtomicUsize::new(0),
         binance_history:AtomicUsize::new(0),
         okx_history:AtomicUsize::new(0)}
 }
 fn count(&self)->usize{self.total.load(Ordering::SeqCst)}
}
impl HttpExecutor for Trade2D2EHistoryGet{
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
    assert_eq!(req.method,HttpMethod::Get);
    assert!(!req.mutation&&req.body.is_none());
    self.total.fetch_add(1,Ordering::SeqCst);
    let url=url::Url::parse(&req.url).unwrap();
    let path=url.path();
    let binance=path=="/fapi/v1/allAlgoOrders";
    let okx=path=="/api/v5/trade/orders-algo-history";
    if !binance&&!okx{return self.prior.execute_request(req)}
    let q=url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
    let n=if binance{
        assert!(!q.contains_key("symbol")&&!q.contains_key("algoType"));
        assert_eq!(q.get("limit").map(|v|v.as_ref()),Some("1000"));
        let start=q.get("startTime").unwrap().parse::<u64>().unwrap();
        let end=q.get("endTime").unwrap().parse::<u64>().unwrap();
        assert_eq!(end-start,6*24*60*60*1000);
        self.binance_history.fetch_add(1,Ordering::SeqCst)
    }else{
        assert!(!q.contains_key("instType")&&!q.contains_key("instId"));
        assert_eq!(q.get("limit").map(|v|v.as_ref()),Some("100"));
        let kind=q.get("ordType").unwrap().as_ref();
        let state=q.get("state").unwrap().as_ref();
        assert!(spotarb_native_market_core::native_dryrun_pending_algos::
          OKX_PENDING_ALGO_TYPES.contains(&kind));
        assert!(["effective","canceled","order_failed"].contains(&state));
        self.okx_history.fetch_add(1,Ordering::SeqCst)
    };
    let kind=q.get("ordType").map(|x|x.as_ref()).unwrap_or("");
    let state=q.get("state").map(|x|x.as_ref()).unwrap_or("");
    let (status,body)=match (self.mode,binance,kind,state,n){
        (1,true,_,_,_)=>(
            200,r#"[{"algoId":311,"symbol":"ZECUSDT","algoStatus":"TRIGGERED",
                "orderId":9021}]"#.to_string()),
        (2,false,"conditional","effective",_)=>(
            200,r#"{"code":"0","data":[{"algoId":"312","instId":"ZEC-USDT",
                 "ordType":"conditional","state":"effective",
                 "ordIdList":["9021"]}]}"#.to_string()),
        (3,false,"oco","canceled",_)=>(
            200,r#"{"code":"0","data":[{"algoId":"313","instId":"BTC-USDT",
                 "state":"canceled","ordIdList":[]}]}"#.to_string()),
        (4,false,"twap","order_failed",_)=>(
            200,r#"{"code":"0","data":[{"algoId":"314","instId":"ETH-USDT",
                 "state":"order_failed"}]}"#.to_string()),
        (5,true,_,_,_)=>(
            403,r#"{"code":-2015}"#.to_string()),
        (6,true,_,_,_)=>(
            200,r#"{"total":0,"orders":[]}"#.to_string()),
        (7,false,"iceberg","canceled",_)=>(
            200,r#"{"code":"50000","data":[]}"#.to_string()),
        (8,false,"smart_iceberg","effective",_)=>(
            200,r#"{"code":"0","data":{}}"#.to_string()),
        (9,true,_,_,n) if n>=1=>(
            200,r#"[{"algoId":315,"symbol":"BNTUSDT","algoStatus":"FINISHED"}]"#.to_string()),
        (10,false,"conditional","effective",n) if n>=24=>(
            200,r#"{"code":"0","data":[{"algoId":"316","state":"effective"}]}"#.to_string()),
        _=>if binance{(200,"[]".to_string())}
           else{(200,r#"{"code":"0","data":[]}"#.to_string())},
    };
    Ok(HttpResponse{status,headers:vec![],body})
 }
}
fn trade2d2e_run(
    store:&NativeStateStore,gen:u64,at:u64,http:&Trade2D2EHistoryGet,
)->Result<spotarb_native_market_core::native_dryrun_algo_history::BoundedAlgoHistoryReview,
          spotarb_native_market_core::native_dryrun_algo_history::AlgoHistoryError>{
 use spotarb_native_market_core::{
    native_dryrun_algo_history::inspect_bounded_mock_algo_history,
    private_rest::BinanceCredentials,
 };
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 inspect_bounded_mock_algo_history(http,store,gen,&okx,&bn,at,OkxDomain::Global,10_000)
}


#[test]
fn trade2d2e_96_historical_gets_zero_results_remain_only_bounded_simulated_diagnostics(){
 let (dir,store,state,at)=trade2d2b_seed();
 let api=Trade2D2EHistoryGet::new(0,at,trade2d2c_baseline(&state));
 let original=std::fs::read(dir.join("state.json")).unwrap();
 let res=trade2d2e_run(&store,state.generation,at,&api).unwrap();
 assert_eq!(api.count(),96);
 assert_eq!(api.binance_history.load(Ordering::SeqCst),2);
 assert_eq!(api.okx_history.load(Ordering::SeqCst),48);
 assert_eq!(res.synthetic_signed_gets,96);
 assert_eq!(res.history_scans,2);
 assert!(res.binance_unfiltered_recent_history_empty_in_both
         &&res.okx_known_state_type_history_empty_in_both);
 assert_eq!(res.okx_combinations_checked,24);
 assert_eq!(res.binance_window_end_ms-res.binance_window_start_ms,
    6*24*60*60*1000);
 assert!(!res.known_old_triggered_child_orders_proven_absent
    &&!res.across_all_history_and_account_transfers_proven_complete
    &&!res.full_lifetime_pnl_proven
    &&!res.actual_live_close_proven
    &&!res.may_cancel_retry_or_send_order
    &&!res.may_verify_closed);
 assert_eq!(res.previous.total_mock_signed_gets,46);
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2d2e_any_triggered_canceled_failed_algo_history_on_foreign_symbol_denies(){
 for (label,mode) in [
    ("Binance already triggered on ZEC",1),
    ("OKX effective foreign ZEC spawned child",2),
    ("OKX harmless-looking canceled record",3),
    ("OKX historical TWAP failure",4),
    ("Binance private GET unauthorized",5),
    ("Binance history bad envelope",6),
    ("OKX algo history business code failure",7),
    ("OKX bad history JSON data",8),
    ("Binance history appears on second scan",9),
    ("OKX new algo effective on second scan",10),
 ]{
     let (dir,store,state,at)=trade2d2b_seed();
     let api=Trade2D2EHistoryGet::new(mode,at,trade2d2c_baseline(&state));
     let bytes=std::fs::read(dir.join("state.json")).unwrap();
     assert!(trade2d2e_run(&store,state.generation,at,&api).is_err(),
        "dangerous historical algo was ignored: {label}");
     assert!(api.count()<=96);
     assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
     std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2d2e_stale_generation_missing_manual_latch_or_bad_window_no_exchange_get(){
 let (dir,store,state,at)=trade2d2b_seed();
 let api=Trade2D2EHistoryGet::new(0,at,trade2d2c_baseline(&state));
 assert!(trade2d2e_run(&store,state.generation-1,at,&api).is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_http,gen)=trade1_quarantine_c6();
 let api=Trade2D2EHistoryGet::new(0,at,"0".into());
 assert!(trade2d2e_run(&store,gen,at,&api).is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2d2e_owner_revision_during_second_historical_sweep_invalidates_all_zero_report(){
 use spotarb_native_market_core::{
    native_dryrun_algo_history::inspect_bounded_mock_algo_history,
    private_rest::BinanceCredentials,
 };
 let (dir,store,state,at)=trade2d2b_seed();
 struct Concurrent<'a>{
    base:Trade2D2EHistoryGet,store:&'a NativeStateStore,
    expected:u64,advanced:AtomicUsize,
 }
 impl HttpExecutor for Concurrent<'_>{
    fn execute_request(&self,r:&HttpsRequest)->Result<HttpResponse,TransportError>{
        let path=url::Url::parse(&r.url).unwrap();
        if path.path()=="/fapi/v1/allAlgoOrders"
           &&self.base.binance_history.load(Ordering::SeqCst)==1
           &&self.advanced.fetch_add(1,Ordering::SeqCst)==0{
           let (saved,_)=self.store.transact(self.expected,|_|Ok(())).unwrap();
           assert_eq!(saved.generation,self.expected+1);
        }
        self.base.execute_request(r)
    }
 }
 let api=Concurrent{
    base:Trade2D2EHistoryGet::new(0,at,trade2d2c_baseline(&state)),
    store:&store,expected:state.generation,advanced:AtomicUsize::new(0),
 };
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 assert!(inspect_bounded_mock_algo_history(
     &api,&store,state.generation,&okx,&bn,at,OkxDomain::Global,10_000
 ).is_err());
 assert_eq!(api.base.count(),96);
 assert_eq!(api.advanced.load(Ordering::SeqCst),1);
 assert_eq!(store.load().unwrap().unwrap().generation,state.generation+1);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2D2E2 simulated one-parent/one-child/bounded trade+fee proof.
// No real account signed GET, no POST or DELETE, no VerifiedClosed.
struct Trade2D2E2Mock {
    mode:usize,now:u64,
    count:AtomicUsize,
    okx_parent:AtomicUsize,
    future_fills:AtomicUsize,
}
impl Trade2D2E2Mock{
    fn new(mode:usize,now:u64)->Self{
      Self{mode,now,count:AtomicUsize::new(0),
          okx_parent:AtomicUsize::new(0),future_fills:AtomicUsize::new(0)}
    }
    fn n(&self)->usize{self.count.load(Ordering::SeqCst)}
}
impl HttpExecutor for Trade2D2E2Mock{
 fn execute_request(&self,r:&HttpsRequest)->Result<HttpResponse,TransportError>{
    assert_eq!(r.method,HttpMethod::Get);
    assert!(!r.mutation&&r.body.is_none());
    self.count.fetch_add(1,Ordering::SeqCst);
    let url=url::Url::parse(&r.url).unwrap();
    let path=url.path();
    let q=url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
    let now=self.now;
    let ok_read=if path=="/api/v5/trade/order-algo"{
       self.okx_parent.fetch_add(1,Ordering::SeqCst)
    }else{0};
    let bn_read=if path=="/fapi/v1/userTrades"
       &&q.get("fromId").map(|x|x.as_ref())==Some("0"){
       self.future_fills.fetch_add(1,Ordering::SeqCst)
    }else{0};
    let mut result=match path{
      "/api/v5/trade/order-algo"=>{
        assert_eq!(q.get("algoId").map(|x|x.as_ref()),Some("6001"));
        serde_json::json!({"code":"0","data":[{
          "algoId":"6001","instId":"BNT-USDT","side":"sell","tdMode":"cash",
          "state":"effective","sz":"1","triggerTime":(now-500).to_string(),
          "ordIdList":["9001"],"subAlgoIdList":[]}]})
      }
      "/api/v5/trade/order"=>{
        if self.mode!=6{
          assert_eq!(q.get("ordId").map(|x|x.as_ref()),Some("9001"));
        }
        serde_json::json!({"code":"0","data":[{
          "ordId":"9001","algoId":"6001","instId":"BNT-USDT",
          "side":"sell","tdMode":"cash","state":"filled",
          "sz":"1","accFillSz":"1"}]})
      }
      "/api/v5/trade/fills-history"=>{
        assert_eq!(q.get("ordId").map(|x|x.as_ref()),Some("9001"));
        if q.contains_key("after"){
           serde_json::json!({"code":"0","data":[]})
        }else{
           serde_json::json!({"code":"0","data":[{
              "ordId":"9001","billId":"7001","tradeId":"Tclose",
              "fillPx":"20","fillSz":"1","fee":"-0.04",
              "feeCcy":"USDT","fillTime":(now-100).to_string()}]})
        }
      }
      "/fapi/v1/algoOrder"=>{
        assert_eq!(q.get("algoId").map(|x|x.as_ref()),Some("7001"));
        serde_json::json!({"algoId":7001,"symbol":"BNTUSDT",
             "algoStatus":"TRIGGERED","side":"BUY","positionSide":"BOTH",
             "reduceOnly":true,"quantity":"0.4",
             "triggerTime":now-450,"actualOrderId":"9101"})
      }
      "/fapi/v1/order"=>{
        assert_eq!(q.get("orderId").map(|x|x.as_ref()),Some("9101"));
        serde_json::json!({"orderId":9101,"symbol":"BNTUSDT",
            "side":"BUY","positionSide":"BOTH","reduceOnly":true,
            "origQty":"0.4","executedQty":"0.4","status":"FILLED"})
      }
      "/fapi/v1/userTrades"=>{
        assert_eq!(q.get("orderId").map(|x|x.as_ref()),Some("9101"));
        if q.get("fromId").map(|x|x.as_ref())==Some("0"){
           serde_json::json!([
             {"symbol":"BNTUSDT","id":8001,"orderId":9101,"side":"BUY",
              "positionSide":"BOTH","marginAsset":"USDT","buyer":true,
              "price":"21","qty":"0.2","commission":"0.005",
              "commissionAsset":"USDT","time":now-90},
             {"symbol":"BNTUSDT","id":8002,"orderId":9101,"side":"BUY",
              "positionSide":"BOTH","marginAsset":"USDT","buyer":true,
              "price":"21","qty":"0.2","commission":"0.005",
              "commissionAsset":"USDT","time":now-70}])
        }else{serde_json::json!([])}
      }
      _=>panic!("mock unsanctioned URL {path}")
    };
    match self.mode{
       1 if path=="/api/v5/trade/order-algo"=>
          result["data"][0]["subAlgoIdList"]=serde_json::json!(["8088"]),
       2 if path=="/api/v5/trade/order-algo"=>
          result["data"][0]["ordIdList"]=serde_json::json!(["9001","9002"]),
       3 if path=="/api/v5/trade/order-algo"=>
          result["data"][0]["ordIdList"]=serde_json::json!([]),
       4 if path=="/api/v5/trade/order-algo"=>
          result["data"][0]["state"]=serde_json::json!("canceled"),
       5 if path=="/api/v5/trade/order-algo"=>
          result["data"][0]["triggerTime"]=serde_json::json!("0"),
       6 if path=="/api/v5/trade/order-algo"&&ok_read>0=>
          result["data"][0]["ordIdList"]=serde_json::json!(["9999"]),
       7 if path=="/api/v5/trade/order"=>
          result["data"][0]["algoId"]=serde_json::json!("6002"),
       8 if path=="/api/v5/trade/order"=>
          result["data"][0]["state"]=serde_json::json!("canceled"),
       9 if path=="/api/v5/trade/order"=>
          result["data"][0]["accFillSz"]=serde_json::json!("0.9"),
       10 if path=="/api/v5/trade/fills-history"&&!q.contains_key("after")=>
          result["data"][0]["feeCcy"]=serde_json::json!("BNT"),
       11 if path=="/api/v5/trade/fills-history"&&!q.contains_key("after")=>
          result["data"][0]["fillSz"]=serde_json::json!("0.9"),
       12 if path=="/api/v5/trade/fills-history"&&q.contains_key("after")=>
          result=serde_json::json!({"code":"0","data":[{"ordId":"9001"}]}),
       13 if path=="/fapi/v1/algoOrder"=>
          result["actualOrderId"]=serde_json::json!(""),
       14 if path=="/fapi/v1/algoOrder"=>
          result["reduceOnly"]=serde_json::json!(false),
       15 if path=="/fapi/v1/order"=>
          result["executedQty"]=serde_json::json!("0.3"),
       16 if path=="/fapi/v1/order"=>
          result["reduceOnly"]=serde_json::json!(false),
       17 if path=="/fapi/v1/userTrades"&&q.get("fromId").map(|x|x.as_ref())==Some("0")=>
          result[1]["id"]=serde_json::json!(8001),
       18 if path=="/fapi/v1/userTrades"&&q.get("fromId").map(|x|x.as_ref())==Some("0")=>
          result[1]["qty"]=serde_json::json!("0.19"),
       19 if path=="/fapi/v1/userTrades"&&q.get("fromId").map(|x|x.as_ref())==Some("0")=>
          result[1]["commissionAsset"]=serde_json::json!("BNB"),
       20 if path=="/fapi/v1/userTrades"&&q.get("fromId").map(|x|x.as_ref())==Some("0")&&bn_read>0=>
          result[1]["commission"]=serde_json::json!("0.02"),
       21 if path=="/fapi/v1/userTrades"&&q.get("fromId").map(|x|x.as_ref())!=Some("0")=>
          result=serde_json::json!([{"id":9003}]),
       22 if path=="/fapi/v1/userTrades"&&q.get("fromId").map(|x|x.as_ref())==Some("0")=>
          result[0]["time"]=serde_json::json!(now-900),
       23 if path=="/api/v5/trade/fills-history"&&!q.contains_key("after")=>
          result["data"][0]["ordId"]=serde_json::json!("9002"),
       _=>{}
    };
    if self.mode==24&&path=="/fapi/v1/algoOrder"{
       return Ok(HttpResponse{status:403,headers:vec![],
          body:r#"{"code":-2015}"#.into()})
    }
    Ok(HttpResponse{status:200,headers:vec![],body:result.to_string()})
 }
}
fn trade2d2e2_inspect(
 store:&NativeStateStore,gen:u64,at:u64,http:&Trade2D2E2Mock,
)->Result<spotarb_native_market_core::native_dryrun_child_lineage::BoundedChildLineageReview,
 spotarb_native_market_core::native_dryrun_child_lineage::ChildLineageError>{
 use spotarb_native_market_core::{
    native_dryrun_child_lineage::inspect_bounded_mock_trigger_lineage,
    private_rest::BinanceCredentials,
 };
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 inspect_bounded_mock_trigger_lineage(http,store,gen,&okx,&bn,
    at,OkxDomain::Global,10_000,"6001",7001)
}


#[test]
fn trade2d2e2_double_exact_parent_child_fees_remains_manual_no_verified_close(){
 let (dir,store,old,at)=trade2d2b_seed();
 let mock=Trade2D2E2Mock::new(0,at);
 let original=std::fs::read(dir.join("state.json")).unwrap();
 let review=trade2d2e2_inspect(&store,old.generation,at,&mock).unwrap();
 assert_eq!(mock.n(),16);
 assert_eq!(review.synthetic_signed_get_count,16);
 assert_eq!(review.independent_complete_reads,2);
 assert_eq!(review.okx_parent_id,"6001");
 assert_eq!(review.okx_child_order_id,"9001");
 assert_eq!(review.okx_trade_ids,vec!["Tclose".to_string()]);
 assert_eq!(review.okx_fee_usdt_e12,decimal_to_atoms("0.04").unwrap() as i128);
 assert_eq!(review.binance_parent_id,"7001");
 assert_eq!(review.binance_child_order_id,"9101");
 assert_eq!(review.binance_trade_ids,vec!["8001".to_string(),"8002".to_string()]);
 assert_eq!(review.binance_fee_usdt_e12,decimal_to_atoms("0.01").unwrap() as i128);
 assert!(review.single_child_per_parent_seen_in_mock
     &&review.separate_ordinary_order_and_per_fill_identity_seen_in_mock);
 assert!(!review.global_historical_parent_coverage_complete
     &&!review.untraced_sapi_twap_vp_child_orders_proven_absent
     &&!review.all_parent_children_and_subalgos_proven_resolved
     &&!review.real_order_submission_proven
     &&!review.independent_final_account_flatness_proven
     &&!review.whole_account_funding_transfers_and_lifetime_pnl_proven
     &&!review.may_retry_cancel_send_or_mark_verified_closed);
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
 assert_eq!(store.load().unwrap().unwrap(),old);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2d2e2_missing_nested_parent_child_wrong_fills_and_fees_denied(){
 for (name,mode) in [
   ("OKX nested subAlgoIdList",1),("OKX split ordinary child IDs",2),
   ("OKX parent missing child",3),("OKX canceled not proved",4),
   ("OKX missing parent triggerTime",5),("OKX child mutated next scan",6),
   ("OKX child reverse parent ID mismatch",7),
   ("OKX child canceled",8),("OKX partially filled",9),
   ("OKX fee asset BNT",10),("OKX per-trade partial",11),
   ("OKX pagination extra row",12),("Binance no actualOrderId",13),
   ("Binance parent non-reduceOnly",14),("Binance child partially filled",15),
   ("Binance child non-reduceOnly",16),("Binance duplicate trade ID",17),
   ("Binance missing filled quantity",18),("Binance wrong fee asset",19),
   ("Binance fee drift in second scan",20),
   ("Binance cursor still has trades",21),
   ("Binance fill before trigger",22),
   ("OKX wrong order ID in fills",23),
   ("exchange authentication denied",24)
 ]{
   let (dir,store,st,at)=trade2d2b_seed();
   let mock=Trade2D2E2Mock::new(mode,at);
   let bytes=std::fs::read(dir.join("state.json")).unwrap();
   assert!(trade2d2e2_inspect(&store,st.generation,at,&mock).is_err(),
      "unsafe parent-child lineage accepted: {name}");
   assert!(mock.n()<=16);
   assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
   std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2d2e2_missing_original_latch_and_wrong_generation_zero_get(){
 let (dir,store,s,at)=trade2d2b_seed();
 let mock=Trade2D2E2Mock::new(0,at);
 assert!(trade2d2e2_inspect(&store,s.generation-1,at,&mock).is_err());
 assert_eq!(mock.n(),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_exec,gen)=trade1_quarantine_c6();
 let mock=Trade2D2E2Mock::new(0,at);
 assert!(trade2d2e2_inspect(&store,gen,at,&mock).is_err());
 assert_eq!(mock.n(),0);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2d2e2_owner_changes_during_second_per_fill_lookup_then_manual_only(){
 use spotarb_native_market_core::{
    native_dryrun_child_lineage::inspect_bounded_mock_trigger_lineage,
    private_rest::BinanceCredentials,
 };
 let (dir,store,state,at)=trade2d2b_seed();
 struct Racing<'a>{
    base:Trade2D2E2Mock,
    st:&'a NativeStateStore,gen:u64,modified:AtomicUsize,
 }
 impl HttpExecutor for Racing<'_>{
    fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
       if self.base.n()==15&&self.modified.fetch_add(1,Ordering::SeqCst)==0{
         let (new,_)=self.st.transact(self.gen,|_|Ok(())).unwrap();
         assert_eq!(new.generation,self.gen+1);
       }
       self.base.execute_request(req)
    }
 }
 let mock=Racing{base:Trade2D2E2Mock::new(0,at),st:&store,
     gen:state.generation,modified:AtomicUsize::new(0)};
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 assert!(inspect_bounded_mock_trigger_lineage(
   &mock,&store,state.generation,&okx,&bn,at,OkxDomain::Global,
   10_000,"6001",7001).is_err());
 assert_eq!(mock.base.n(),16);
 assert_eq!(mock.modified.load(Ordering::SeqCst),1);
 assert_eq!(store.load().unwrap().unwrap().generation,state.generation+1);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2d2e2_bad_parent_ids_or_missing_quarantine_rejected_before_first_get(){
 use spotarb_native_market_core::{
    native_dryrun_child_lineage::inspect_bounded_mock_trigger_lineage,
    private_rest::BinanceCredentials,
 };
 let (dir,store,state,at)=trade2d2b_seed();
 let mock=Trade2D2E2Mock::new(0,at);
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 for (gen,okx_id,bin_id) in [
      (state.generation-1,"6001",7001),
      (state.generation,"bad id",7001),
      (state.generation,"6001",0),
 ]{
  assert!(inspect_bounded_mock_trigger_lineage(
    &mock,&store,gen,&okx,&bn,at,OkxDomain::Global,
    10_000,okx_id,bin_id).is_err());
 }
 assert_eq!(mock.n(),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_exec,gen)=trade1_quarantine_c6();
 let mock=Trade2D2E2Mock::new(0,at);
 assert!(inspect_bounded_mock_trigger_lineage(
     &mock,&store,gen,&okx,&bn,at,OkxDomain::Global,
     10_000,"6001",7001).is_err());
 assert_eq!(mock.n(),0);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2D2E3A: 2-3 ordinary OKX child orders, exact split of 1.0 BNT.
// Bounded signed MOCK GET-only; not a real order or VerifiedClosed.
struct Trade2D2E3AMock{
    mode:u8,child_count:usize,now:u64,
    requests:AtomicUsize,parent_reads:AtomicUsize,child_page_reads:AtomicUsize,
}
impl Trade2D2E3AMock{
    fn new(mode:u8,child_count:usize,now:u64)->Self{
      Self{mode,child_count,now,requests:AtomicUsize::new(0),
        parent_reads:AtomicUsize::new(0),child_page_reads:AtomicUsize::new(0)}
    }
    fn n(&self)->usize{self.requests.load(Ordering::SeqCst)}
    fn amount(&self,index:usize)->&'static str{
      if self.child_count==3{
        ["0.5","0.3","0.2"][index]
      }else{
        ["0.6","0.4"][index]
      }
    }
    fn fee(&self,index:usize)->&'static str{
      if self.child_count==3{
        ["-0.02","-0.012","-0.008"][index]
      }else{
        ["-0.024","-0.016"][index]
      }
    }
}
impl HttpExecutor for Trade2D2E3AMock{
 fn execute_request(&self,r:&HttpsRequest)->Result<HttpResponse,TransportError>{
    assert_eq!(r.method,HttpMethod::Get);
    assert!(!r.mutation&&r.body.is_none());
    self.requests.fetch_add(1,Ordering::SeqCst);
    let url=url::Url::parse(&r.url).unwrap();
    let path=url.path();
    let q=url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
    let at=self.now;
    let mut result=match path{
       "/api/v5/trade/order-algo"=>{
          let nth=self.parent_reads.fetch_add(1,Ordering::SeqCst);
          assert_eq!(q.get("algoId").map(|x|x.as_ref()),Some("6001"));
          let mut ids=(0..self.child_count)
             .map(|j|(9001+j).to_string()).collect::<Vec<_>>();
          if self.mode==1{ids.push(ids[0].clone());}
          if self.mode==2{ids.push("9004".to_string());}
          if self.mode==3{ids.pop();}
          if self.mode==4&&nth>0{ids.reverse();ids[0]="9099".into();}
          let mut p=serde_json::json!({"code":"0","data":[{
             "algoId":"6001","instId":"BNT-USDT","tdMode":"cash",
             "side":"sell","state":"effective","sz":"1",
             "triggerTime":(at-500).to_string(),"ordIdList":ids,
             "subAlgoIdList":[]}]});
          if self.mode==5{p["data"][0]["subAlgoIdList"]=serde_json::json!(["8222"])}
          if self.mode==6{p["data"][0]["sz"]=serde_json::json!("1.1")}
          if self.mode==7{p["data"][0]["side"]=serde_json::json!("buy")}
          if self.mode==8{p["data"][0]["subAlgoIdList"]=serde_json::Value::Null}
          p
       }
       "/api/v5/trade/order"=>{
          let child=q.get("ordId").unwrap().parse::<usize>().unwrap();
          let ix=child.saturating_sub(9001).min(self.child_count-1);
          let amount=self.amount(ix);
          let mut v=serde_json::json!({"code":"0","data":[{
             "ordId":child.to_string(),"algoId":"6001","instId":"BNT-USDT",
             "side":"sell","tdMode":"cash","state":"filled",
             "sz":amount,"accFillSz":amount}]});
          if self.mode==9&&ix==1{v["data"][0]["algoId"]=serde_json::json!("6500")}
          if self.mode==10&&ix==1{v["data"][0]["state"]=serde_json::json!("canceled")}
          if self.mode==11&&ix==1{v["data"][0]["sz"]=serde_json::json!("0.2")}
          if self.mode==12&&ix==0{v["data"][0]["accFillSz"]=serde_json::json!("0")}
          if self.mode==13&&ix==0{v["data"][0]["instId"]=serde_json::json!("ZEC-USDT")}
          v
       }
       "/api/v5/trade/fills-history"=>{
          let child=q.get("ordId").unwrap().parse::<usize>().unwrap();
          let ix=child.saturating_sub(9001).min(self.child_count-1);
          if q.contains_key("after"){
            if self.mode==14&&ix==1{
              serde_json::json!({"code":"0","data":[{"tradeId":"unexpected"}]})
            }else{serde_json::json!({"code":"0","data":[]})}
          }else{
            let read_no=self.child_page_reads.fetch_add(1,Ordering::SeqCst);
            let mut v=serde_json::json!({"code":"0","data":[{
              "ordId":child.to_string(),"billId":(7001+ix).to_string(),
              "tradeId":format!("child-fill-{ix}"),
              "fillPx":"20","fillSz":self.amount(ix),"fee":self.fee(ix),
              "feeCcy":"USDT","fillTime":(at-70).to_string()}]});
            if self.mode==15&&ix==1{
               v["data"][0]["tradeId"]=serde_json::json!("child-fill-0")
            }
            if self.mode==16&&ix==1{v["data"][0]["feeCcy"]=serde_json::json!("BNT")}
            if self.mode==17&&ix==1{v["data"][0]["fillSz"]=serde_json::json!("0.1")}
            if self.mode==18&&ix==0{v["data"][0]["fillTime"]=serde_json::json!((at-900).to_string())}
            if self.mode==19&&ix==0{v["data"][0]["fillPx"]=serde_json::json!("0")}
            if self.mode==20&&ix==0&&read_no>=self.child_count{
               v["data"][0]["fee"]=serde_json::json!("-0.06")
            }
            if self.mode==21&&ix==0{v["data"][0]["ordId"]=serde_json::json!("9022")}
            if self.mode==22&&ix==0{v["data"][0]["tradeId"]=serde_json::json!("")}
            if self.mode==23&&ix==0{v["data"]=serde_json::json!([])}
            v
          }
       }
       _=>panic!("unapproved synthetic URL {path}")
    };
    if self.mode==24&&path=="/api/v5/trade/order"{
        return Ok(HttpResponse{status:403,headers:vec![],body:"{}".into()})
    }
    // Unknown endpoint and every mutation already trapped by route and asserts.
    Ok(HttpResponse{status:200,headers:vec![],body:result.to_string()})
 }
}
fn trade2d2e3a_run(
    store:&NativeStateStore,g:u64,at:u64,http:&Trade2D2E3AMock,
)->Result<spotarb_native_market_core::native_dryrun_multi_child::MockMultiChildReport,
 spotarb_native_market_core::native_dryrun_child_lineage::ChildLineageError>{
 use spotarb_native_market_core::native_dryrun_multi_child::
    inspect_mock_multi_okx_ordinary_children;
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 inspect_mock_multi_okx_ordinary_children(http,store,g,&okx,at,
     OkxDomain::Global,"6001")
}


#[test]
fn trade2d2e3a_two_three_child_sums_fees_twice_but_still_manual_only(){
 for count in [2,3]{
    let (dir,store,s,now)=trade2d2b_seed();
    let http=Trade2D2E3AMock::new(0,count,now);
    let disk=std::fs::read(dir.join("state.json")).unwrap();
    let result=trade2d2e3a_run(&store,s.generation,now,&http).unwrap();
    assert_eq!(http.n(),if count==2{14}else{20});
    assert_eq!(result.total_mock_signed_get as usize,http.n());
    assert_eq!(result.matching_scans,2);
    assert_eq!(result.observed_children.len(),count);
    assert_eq!(result.observed_parent_total_qty_e12,decimal_to_atoms("1").unwrap());
    assert_eq!(result.observed_total_quote_fee_e12,decimal_to_atoms("0.04").unwrap() as i128);
    assert!(result.all_declared_simple_ordinary_children_seen_in_mock);
    assert!(!result.nested_algos_proven_exhausted
       &&!result.account_wide_historical_algo_completeness_proven
       &&!result.futures_suborders_complete
       &&!result.fee_and_lifetime_pnl_complete
       &&!result.real_exchange_execution_proven
       &&!result.final_live_positions_flat_proven
       &&!result.may_cancel_retry_send_or_verify_closed);
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),disk);
    std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2d2e3a_extra_or_duplicated_child_nested_mismatch_and_fee_drift_denied(){
 for (label,mode) in [
  ("duplicate child ID",1),("four child IDs",2),("missing child ID",3),
  ("changed second scan child IDs",4),("subAlgoIdList not empty",5),
  ("parent whole quantity mismatched",6),("parent BUY not SELL",7),
  ("missing subAlgoIdList",8),("child reverse parent ID incorrect",9),
  ("child canceled",10),("child qty smaller than actual",11),
  ("child partial execution",12),("child foreign symbol",13),
  ("extra fill after cursor",14),("duplicate trade ID across children",15),
  ("quote fee paid in unsupported base asset",16),
  ("individual fill under original filled qty",17),
  ("fill predates parent trigger",18),("zero fill price",19),
  ("quote fee changes second round",20),("foreign child fill order",21),
  ("missing trade ID",22),("missing fill page",23),("HTTP private error",24),
 ]{
    let (dir,store,s,now)=trade2d2b_seed();
    let http=Trade2D2E3AMock::new(mode,2,now);
    let disk=std::fs::read(dir.join("state.json")).unwrap();
    assert!(trade2d2e3a_run(&store,s.generation,now,&http).is_err(),
       "unsafe split-child case {label} accepted");
    assert!(http.n()<=14);
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),disk);
    std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2d2e3a_wrong_owner_or_missing_original_durable_latch_zero_GET(){
 let (dir,store,s,now)=trade2d2b_seed();
 let http=Trade2D2E3AMock::new(0,2,now);
 assert!(trade2d2e3a_run(&store,s.generation-1,now,&http).is_err());
 assert_eq!(http.n(),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_exec,gen)=trade1_quarantine_c6();
 let http=Trade2D2E3AMock::new(0,2,now);
 assert!(trade2d2e3a_run(&store,gen,now,&http).is_err());
 assert_eq!(http.n(),0);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2d2e3a_native_owner_advancing_during_last_get_invalidates_all_children(){
 use spotarb_native_market_core::native_dryrun_multi_child::
    inspect_mock_multi_okx_ordinary_children;
 let (dir,store,s,at)=trade2d2b_seed();
 struct OwnerRace<'a>{
    inner:Trade2D2E3AMock,store:&'a NativeStateStore,
    generation:u64,changed:AtomicUsize,
 }
 impl HttpExecutor for OwnerRace<'_>{
    fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
       if self.inner.n()==13&&self.changed.fetch_add(1,Ordering::SeqCst)==0{
           let (new,_)=self.store.transact(self.generation,|_|Ok(())).unwrap();
           assert_eq!(new.generation,self.generation+1);
       }
       self.inner.execute_request(req)
    }
 }
 let mock=OwnerRace{
    inner:Trade2D2E3AMock::new(0,2,at),
    store:&store,generation:s.generation,
    changed:AtomicUsize::new(0),
 };
 let creds=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 assert!(inspect_mock_multi_okx_ordinary_children(
    &mock,&store,s.generation,&creds,at,OkxDomain::Global,"6001").is_err());
 assert_eq!(mock.inner.n(),14);
 assert_eq!(mock.changed.load(Ordering::SeqCst),1);
 assert_eq!(store.load().unwrap().unwrap().generation,s.generation+1);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2d2e3a_bad_parent_identifier_or_missing_unknown_status_makes_zero_get(){
 use spotarb_native_market_core::native_dryrun_multi_child::
    inspect_mock_multi_okx_ordinary_children;
 let (dir,store,s,now)=trade2d2b_seed();
 let creds=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let mock=Trade2D2E3AMock::new(0,2,now);
 assert!(inspect_mock_multi_okx_ordinary_children(
    &mock,&store,s.generation,&creds,now,OkxDomain::Global,"bad 6001").is_err());
 assert_eq!(mock.n(),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_source,g)=trade1_quarantine_c6();
 let mock=Trade2D2E3AMock::new(0,2,now);
 assert!(inspect_mock_multi_okx_ordinary_children(
    &mock,&store,g,&creds,now,OkxDomain::Global,"6001").is_err());
 assert_eq!(mock.n(),0);
 std::fs::remove_dir_all(dir).unwrap();
}




// TRADE2D2E3B documents exchange restriction: OKX advanceOrdType chase
// is only available to FUTURES / SWAP, NEVER OKX SPOT. Do not create an
// invented positive simulated SPOT chase nested scenario.
struct Trade2D2E3BParentShape<E>{
    mock:E,issue:usize,received:AtomicUsize,
}
impl<E:HttpExecutor> HttpExecutor for Trade2D2E3BParentShape<E>{
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
    let mut response=self.mock.execute_request(req)?;
    let path=url::Url::parse(&req.url).unwrap();
    if path.path()!="/api/v5/trade/order-algo"{
        return Ok(response)
    }
    self.received.fetch_add(1,Ordering::SeqCst);
    let mut v:serde_json::Value=serde_json::from_str(&response.body).unwrap();
    let row=&mut v["data"][0];
    match self.issue{
        0=>{
           row["instType"]=serde_json::json!("SPOT");
           row["ordType"]=serde_json::json!("trigger");
           row["advanceOrdType"]=serde_json::json!("");
           row["tgtCcy"]=serde_json::json!("base_ccy");
        }
        1=>row["advanceOrdType"]=serde_json::json!("chase"),
        2=>row["ordType"]=serde_json::json!("chase"),
        3=>row["instType"]=serde_json::json!("SWAP"),
        4=>row["instType"]=serde_json::json!("FUTURES"),
        5=>row["tgtCcy"]=serde_json::json!("quote_ccy"),
        6=>row["advanceOrdType"]=serde_json::json!({"invalid":"type"}),
        7=>row["ordType"]=serde_json::json!(null),
        8=>row["instType"]=serde_json::json!(""),
        9=>row["tgtCcy"]=serde_json::json!("random_symbol"),
        10=>row["advanceOrdType"]=serde_json::json!("fok"),
        11=>row["advanceOrdType"]=serde_json::json!("ioc"),
        _=>unreachable!(),
    }
    response.body=v.to_string();
    Ok(response)
 }
}
#[test]
fn trade2d2e3b_spot_chase_and_nonbase_spot_unit_hard_denied_in_both_existing_auditors(){
 use spotarb_native_market_core::{
    native_dryrun_child_lineage::inspect_bounded_mock_trigger_lineage,
    native_dryrun_multi_child::inspect_mock_multi_okx_ordinary_children,
    private_rest::BinanceCredentials,
 };
 for (label,mode) in [
    ("SPOT trigger-chase is not an OKX feature",1),
    ("SPOT algo ordType chase is FUTURES/SWAP-only",2),
    ("forged SPOT instId with SWAP instType",3),
    ("forged SPOT instId with FUTURES instType",4),
    ("SPOT sell expressed in quote currency",5),
    ("malformed advanceOrdType object",6),
    ("malformed ordType null",7),
    ("empty claimed instType",8),
    ("unknown SPOT sell unit",9),
 ]{
    let (dir,store,state,at)=trade2d2b_seed();
    let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    let disk=std::fs::read(dir.join("state.json")).unwrap();
    let simple=Trade2D2E3BParentShape{
        mock:Trade2D2E2Mock::new(0,at),issue:mode,
        received:AtomicUsize::new(0),
    };
    assert!(inspect_bounded_mock_trigger_lineage(
         &simple,&store,state.generation,&okx,&bn,at,OkxDomain::Global,
         10_000,"6001",7001).is_err(),"{label}: E2 improperly accepted");
    assert_eq!(simple.mock.n(),1,"{label}: E2 must not query child after invalid SPOT parent");
    let split=Trade2D2E3BParentShape{
        mock:Trade2D2E3AMock::new(0,2,at),issue:mode,
        received:AtomicUsize::new(0),
    };
    assert!(inspect_mock_multi_okx_ordinary_children(
        &split,&store,state.generation,&okx,at,OkxDomain::Global,"6001"
    ).is_err(),"{label}: E3A improperly accepted");
    assert_eq!(split.mock.n(),1,"{label}: E3A must not query child after invalid SPOT parent");
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),disk);
    std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2d2e3b_valid_explicit_spot_type_and_base_unit_keep_original_strict_no_trade_logic(){
 use spotarb_native_market_core::{
    native_dryrun_child_lineage::inspect_bounded_mock_trigger_lineage,
    native_dryrun_multi_child::inspect_mock_multi_okx_ordinary_children,
    private_rest::BinanceCredentials,
 };
 for unit_mode in [0,10,11]{
    let (dir,store,state,at)=trade2d2b_seed();
    let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
    let disk=std::fs::read(dir.join("state.json")).unwrap();
    let simple=Trade2D2E3BParentShape{
        mock:Trade2D2E2Mock::new(0,at),issue:unit_mode,
        received:AtomicUsize::new(0),
    };
    let one=inspect_bounded_mock_trigger_lineage(
        &simple,&store,state.generation,&okx,&bn,at,OkxDomain::Global,
        10_000,"6001",7001).unwrap();
    assert_eq!(simple.mock.n(),16);
    assert!(!one.may_retry_cancel_send_or_mark_verified_closed);
    let split=Trade2D2E3BParentShape{
        mock:Trade2D2E3AMock::new(0,2,at),issue:unit_mode,
        received:AtomicUsize::new(0),
    };
    let two=inspect_mock_multi_okx_ordinary_children(
        &split,&store,state.generation,&okx,at,OkxDomain::Global,"6001"
    ).unwrap();
    assert_eq!(split.mock.n(),14);
    assert!(!two.may_cancel_retry_send_or_verify_closed);
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),disk);
    std::fs::remove_dir_all(dir).unwrap();
 }
}


// TRADE2F2A: wholly synthetic signed GET all-income and two OKX currency
// bill chains, double-read. No real funding/transfer attestations.
struct Trade2F2AGet{
    mode:u8,at:u64,
    requests:AtomicUsize,
    income_reads:AtomicUsize,base_reads:AtomicUsize,quote_reads:AtomicUsize,
}
impl Trade2F2AGet{
    fn new(mode:u8,at:u64)->Self{
        Self{mode,at,requests:AtomicUsize::new(0),
             income_reads:AtomicUsize::new(0),
             base_reads:AtomicUsize::new(0),
             quote_reads:AtomicUsize::new(0)}
    }
    fn count(&self)->usize{self.requests.load(Ordering::SeqCst)}
}
impl HttpExecutor for Trade2F2AGet{
 fn execute_request(&self,request:&HttpsRequest)->Result<HttpResponse,TransportError>{
    assert_eq!(request.method,HttpMethod::Get);
    assert!(!request.mutation&&request.body.is_none());
    self.requests.fetch_add(1,Ordering::SeqCst);
    let url=url::Url::parse(&request.url).unwrap();
    let query=url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
    let path=url.path();
    let n=if path=="/fapi/v1/income"{
        assert!(!query.contains_key("symbol")&&!query.contains_key("incomeType"));
        self.income_reads.fetch_add(1,Ordering::SeqCst)
    }else if path=="/api/v5/account/bills-archive"{
        if query.get("ccy").map(|v|v.as_ref())==Some("BNT"){
            self.base_reads.fetch_add(1,Ordering::SeqCst)
        }else{
            assert_eq!(query.get("ccy").map(|v|v.as_ref()),Some("USDT"));
            self.quote_reads.fetch_add(1,Ordering::SeqCst)
        }
    }else{
        panic!("unexpected signed request path {path}")
    };
    let now=self.at;
    let after=query.contains_key("after");
    let page=query.get("page").map(|v|v.as_ref()).unwrap_or("1");
    let mut val=match path{
        "/fapi/v1/income" if page=="1"=>serde_json::json!([
            {"symbol":"BNTUSDT","incomeType":"FUNDING_FEE",
             "income":"-0.03","asset":"USDT","time":now-100,
             "tranId":101,"tradeId":""},
            {"symbol":"","incomeType":"TRANSFER",
             "income":"10","asset":"USDT","time":now-90,
             "tranId":101,"tradeId":""},
            {"symbol":"BNTUSDT","incomeType":"COMMISSION",
             "income":"-0.01","asset":"USDT","time":now-80,
             "tranId":102,"tradeId":"1234"},
            {"symbol":"BNTUSDT","incomeType":"REALIZED_PNL",
             "income":"-0.05","asset":"USDT","time":now-75,
             "tranId":103,"tradeId":"1234"}
        ]),
        "/fapi/v1/income"=>serde_json::json!([]),
        "/api/v5/account/bills-archive" if !after=>{
            if query.get("ccy").map(|v|v.as_ref())==Some("BNT"){
                serde_json::json!({"code":"0","data":[
                    {"billId":"8002","ccy":"BNT","bal":"0.125",
                     "balChg":"-1","ts":now-70,"type":"2","subType":"2",
                     "ordId":"3301","tradeId":"5678"},
                    {"billId":"8001","ccy":"BNT","bal":"1.125",
                     "balChg":"1","ts":now-180,"type":"2","subType":"1",
                     "ordId":"2201","tradeId":"1234"}
                ]})
            }else{
                serde_json::json!({"code":"0","data":[
                    {"billId":"9002","ccy":"USDT","bal":"100",
                     "balChg":"20","ts":now-70,"type":"2","subType":"2",
                     "ordId":"3301","tradeId":"5678"},
                    {"billId":"9001","ccy":"USDT","bal":"80",
                     "balChg":"-20","ts":now-180,"type":"2","subType":"1",
                     "ordId":"2201","tradeId":"1234"}
                ]})
            }
        },
        "/api/v5/account/bills-archive"=>serde_json::json!({"code":"0","data":[]}),
        _=>unreachable!(),
    };
    match self.mode{
       1 if path=="/fapi/v1/income"&&page=="1"=>
         val[1]["tranId"]=serde_json::json!(101),
       // Duplicate SAME class+id is unsafe, unlike sharing ID across classes.
       2 if path=="/fapi/v1/income"&&page=="1"=>{
         val[2]["incomeType"]=serde_json::json!("FUNDING_FEE");
         val[2]["tranId"]=serde_json::json!(101);
       },
       3 if path=="/fapi/v1/income"&&page=="1"=>
         val[2]["incomeType"]=serde_json::json!("UNKNOWN_REWARD"),
       4 if path=="/fapi/v1/income"&&page=="1"=>
         val[0]["asset"]=serde_json::json!("BNB"),
       5 if path=="/fapi/v1/income"&&page=="1"=>
         val[0]["symbol"]=serde_json::json!("ZECUSDT"),
       6 if path=="/fapi/v1/income"&&page=="1"&&n>=2=>
         val[0]["income"]=serde_json::json!("-0.04"),
       7 if path=="/fapi/v1/income"&&page!="1"=>
         val=serde_json::json!([{"missing":"pagination-not-complete"}]),
       8 if path=="/api/v5/account/bills-archive"&&!after&&query.get("ccy").map(|v|v.as_ref())==Some("BNT")=>
         val["data"][1]["billId"]=serde_json::json!("9000"),
       9 if path=="/api/v5/account/bills-archive"&&!after&&query.get("ccy").map(|v|v.as_ref())==Some("USDT")=>
         val["data"][1]["bal"]=serde_json::json!("81"),
       10 if path=="/api/v5/account/bills-archive"&&!after&&query.get("ccy").map(|v|v.as_ref())==Some("BNT")=>
         val["data"][0]["ccy"]=serde_json::json!("ZEC"),
       11 if path=="/api/v5/account/bills-archive"&&!after=>
         val["code"]=serde_json::json!("50001"),
       12 if path=="/api/v5/account/bills-archive"&&after=>
         val["data"]=serde_json::json!([{"billId":"7777"}]),
       13 if path=="/api/v5/account/bills-archive"&&!after&&n>=2=>
         val["data"][0]["balChg"]=serde_json::json!("11"),
       14 if path=="/api/v5/account/bills-archive"&&!after=>
         val["data"][0]["ts"]=serde_json::json!(now+60_000),
       15 if path=="/fapi/v1/income"&&page=="1"=>
         val[1]["symbol"]=serde_json::json!("BNTUSDT"),
       16 if path=="/fapi/v1/income"&&page=="1"=>
         val[0]["income"]=serde_json::json!("not-decimal"),
       17 if path=="/fapi/v1/income"&&page=="1"=>
         val[0]["tranId"]=serde_json::json!(null),
       18 if path=="/api/v5/account/bills-archive"&&!after=>
         val["data"][0]["balChg"]=serde_json::json!("not-decimal"),
       21 if path=="/api/v5/account/bills-archive"&&!after
          &&query.get("ccy").map(|v|v.as_ref())==Some("BNT")=>{
         val["data"][0]["fee"]=serde_json::json!("-0.0001");
         val["data"][0]["instType"]=serde_json::json!("SPOT");
         val["data"][0]["instId"]=serde_json::json!("BNT-USDT");
       },
       22 if path=="/api/v5/account/bills-archive"&&!after
          &&query.get("ccy").map(|v|v.as_ref())==Some("USDT")=>{
         val["data"][0]["fee"]=serde_json::json!("0.01");
       },
       23 if path=="/api/v5/account/bills-archive"&&!after=>{
         val["data"][0]["instType"]=serde_json::json!("SWAP");
       },
       24 if path=="/api/v5/account/bills-archive"&&!after=>{
         val["data"][0]["instId"]=serde_json::json!("ZEC-USDT");
       },
       25 if path=="/api/v5/account/bills-archive"&&!after=>{
         val["data"][0]["fee"]=serde_json::json!("invalid");
       },
       26 if path=="/api/v5/account/bills-archive"&&!after
          &&query.get("ccy").map(|v|v.as_ref())==Some("USDT")=>{
         val["data"][0]["fee"]=serde_json::json!("-0.01");
         val["data"][0]["instType"]=serde_json::json!("SPOT");
         val["data"][0]["instId"]=serde_json::json!("BNT-USDT");
       },
       27 if path=="/api/v5/account/bills-archive"&&!after
          &&query.get("ccy").map(|v|v.as_ref())==Some("USDT")=>{
         val["data"][0]["fee"]=serde_json::json!("0");
         val["data"][0]["instType"]=serde_json::json!("SPOT");
         val["data"][0]["instId"]=serde_json::json!("BNT-USDT");
       },
       _=>{},
    }
    if self.mode==19&&path=="/fapi/v1/income"{
        return Ok(HttpResponse{status:403,headers:vec![],body:"{}".into()})
    }
    if self.mode==20&&path=="/api/v5/account/bills-archive"{
        return Ok(HttpResponse{status:200,headers:vec![],body:"not-json".into()})
    }
    Ok(HttpResponse{status:200,headers:vec![],body:val.to_string()})
 }
}
fn trade2f2a_inspect(
 store:&NativeStateStore,g:u64,now:u64,api:&Trade2F2AGet
)->Result<spotarb_native_market_core::native_trade2f_income_window::BoundedMoneyWindowReport,
spotarb_native_market_core::native_trade2f_income_window::MoneyWindowError>{
 use spotarb_native_market_core::{
    native_trade2f_income_window::inspect_bounded_mock_cash_history,
    private_rest::BinanceCredentials,
 };
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bin=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 inspect_bounded_mock_cash_history(
     api,store,g,&okx,&bin,now,OkxDomain::Global,10_000)
}


#[test]
fn trade2f2a_exact_bounded_funding_and_two_okx_currency_bill_chains_is_no_lifetime_pnl(){
 let (dir,store,state,at)=trade2d2b_seed();
 let api=Trade2F2AGet::new(0,at);
 let original=std::fs::read(dir.join("state.json")).unwrap();
 let review=trade2f2a_inspect(&store,state.generation,at,&api).unwrap();
 assert_eq!(api.count(),12,"2 scans x (2 Binance+2 base+2 USDT)");
 assert_eq!(review.signed_fake_gets,12);
 assert_eq!(review.verified_identical_scans,2);
 assert_eq!(review.binance_income_rows,4);
 assert_eq!(review.okx_base_bill_rows,2);
 assert_eq!(review.okx_quote_bill_rows,2);
 assert_eq!(review.binance_funding_rows,1);
 assert_eq!(review.observed_binance_funding_signed_usdt_e12,
       spotarb_native_market_core::fill_economics::signed_decimal_to_atoms("-0.03").unwrap());
 assert_eq!(review.observed_binance_transfer_rows,1);
 assert_eq!(review.observed_binance_transfer_signed_usdt_e12,
       spotarb_native_market_core::fill_economics::signed_decimal_to_atoms("10").unwrap());
 assert_eq!(review.observed_okx_base_signed_change_e12,0);
 assert_eq!(review.observed_okx_usdt_signed_change_e12,0);
 assert!(!review.binance_trade_commissions_and_realized_pnl_matched_to_four_legs
    &&!review.binance_transfers_proven_external_not_internal
    &&!review.okx_bill_types_matched_to_all_four_trade_fills
    &&!review.all_future_funding_between_open_and_close_proven
    &&!review.complete_cash_deposits_withdrawals_proven
    &&!review.full_lifetime_profit_proven
    &&!review.no_late_income_or_new_order_proven
    &&!review.may_send_cancel_retry_or_verify_closed);
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
 assert_eq!(store.load().unwrap().unwrap(),state);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f2a_income_type_aware_tranid_uniqueness_still_detects_duplicate_in_same_class(){
 let (dir,store,state,at)=trade2d2b_seed();
 let api=Trade2F2AGet::new(2,at);
 assert!(trade2f2a_inspect(&store,state.generation,at,&api).is_err());
 assert_eq!(api.count(),1);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f2a_corrupted_income_bill_retention_fee_and_transfer_history_fail_closed(){
 for (name,mode) in [
   ("unexpected income class",3),("non USDT fee asset",4),
   ("foreign futures funding",5),("changed funding second scan",6),
   ("Binance pagination not empty",7),("nonmonotonic OKX bill IDs",8),
   ("OKX bill balance chain discontinuity",9),("wrong bill currency",10),
   ("OKX nonzero error code",11),("OKX tail never empty",12),
   ("OKX history changed between scans",13),
   ("future timestamp",14),("transfer has wrong symbol",15),
   ("bad decimal",16),("absent transaction identity",17),
   ("broken OKX bill amount",18),("Binance HTTP 403",19),
   ("OKX history non JSON",20)
 ]{
     let (dir,store,state,at)=trade2d2b_seed();
     let api=Trade2F2AGet::new(mode,at);
     let prior=std::fs::read(dir.join("state.json")).unwrap();
     assert!(trade2f2a_inspect(&store,state.generation,at,&api).is_err(),
         "{name}: unsafe account data accepted");
     assert!(api.count()<=24);
     assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),prior);
     std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2f2a_stale_source_generation_or_missing_entry_never_performs_private_get(){
 let (dir,store,state,at)=trade2d2b_seed();
 let api=Trade2F2AGet::new(0,at);
 assert!(trade2f2a_inspect(&store,state.generation-1,at,&api).is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_exec,gen)=trade1_quarantine_c6();
 let api=Trade2F2AGet::new(0,at);
 // Source C6 state has an original entry; a valid read is not a real
 // close account proof. Wrong owner remains a mandatory zero-GET deny.
 assert!(trade2f2a_inspect(&store,gen-1,at,&api).is_err());
 assert_eq!(api.count(),0);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2f2a_concurrent_native_generation_change_during_last_cash_get_denied(){
 use spotarb_native_market_core::{
    native_trade2f_income_window::inspect_bounded_mock_cash_history,
    private_rest::BinanceCredentials,
 };
 let (dir,store,state,at)=trade2d2b_seed();
 struct Racing<'a>{
    mock:Trade2F2AGet,store:&'a NativeStateStore,gen:u64,changed:AtomicUsize,
 }
 impl HttpExecutor for Racing<'_>{
    fn execute_request(&self,r:&HttpsRequest)->Result<HttpResponse,TransportError>{
        if self.mock.count()==11&&self.changed.fetch_add(1,Ordering::SeqCst)==0{
            let (new,_)=self.store.transact(self.gen,|_|Ok(())).unwrap();
            assert_eq!(new.generation,self.gen+1);
        }
        self.mock.execute_request(r)
    }
 }
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKESECRET").unwrap();
 let api=Racing{
    mock:Trade2F2AGet::new(0,at),store:&store,gen:state.generation,
    changed:AtomicUsize::new(0),
 };
 assert!(inspect_bounded_mock_cash_history(&api,&store,state.generation,
    &okx,&bn,at,OkxDomain::Global,10_000).is_err(),
    "race must invalidate independent cash history snapshots");
 assert_eq!(api.mock.count(),12);
 assert_eq!(api.changed.load(Ordering::SeqCst),1);
 assert_eq!(store.load().unwrap().unwrap().generation,state.generation+1);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2f2c_realistic_c6_one_spot_vs_point_four_short_and_unknown_mock_latch_zero_get(){
 use spotarb_native_market_core::{
    native_trade2f_owner_binding::inspect_owner_bound_mock_four_leg_income,
    private_rest::BinanceCredentials,
 };
 let (dir,store,state,at)=trade2d2b_seed();
 let fake=Trade2F2AGet::new(0,at);
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let okx=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
 let bn=BinanceCredentials::new("FAKEBINKEY","FAKEBINSECRET").unwrap();
 let result=inspect_owner_bound_mock_four_leg_income(
    &fake,&store,state.generation,&okx,&bn,at,OkxDomain::Global,10_000);
 assert!(result.is_err(),"C6 mock close cannot acquire account PnL status");
 assert_eq!(fake.count(),0,"known 1.0 vs 0.4 BNT must abort BEFORE exchange GET");
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 assert_eq!(store.load().unwrap().unwrap(),state);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2f2d_official_okx_base_fee_rebate_wrong_insttype_instid_or_amount_fail_before_cash_profit(){
 for (what,mode) in [
   ("fee paid in BNT cannot be priced as USDT",21u8),
   ("OKX positive fee rebate needs separate accounting",22),
   ("foreign derivatives instrument type on spot bill",23),
   ("foreign ZEC spot bill on BNT cycle",24),
   ("unparseable fee in original bill",25),
 ]{
    let (dir,store,state,at)=trade2d2b_seed();
    let before=std::fs::read(dir.join("state.json")).unwrap();
    let fake=Trade2F2AGet::new(mode,at);
    assert!(trade2f2a_inspect(&store,state.generation,at,&fake).is_err(),
       "unsafe actual exchange-shaped OKX bill condition: {what}");
    assert!(fake.count()<=6);
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
    std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2f2d_okx_explicit_zero_bill_fee_and_spot_instrument_keep_double_read(){
 let (dir,store,state,at)=trade2d2b_seed();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let unsafe_fee=Trade2F2AGet::new(26,at);
 assert!(trade2f2a_inspect(&store,state.generation,at,&unsafe_fee).is_err());
 let fake=Trade2F2AGet::new(27,at);
 let report=trade2f2a_inspect(&store,state.generation,at,&fake).unwrap();
 assert_eq!(fake.count(),12);
 assert_eq!(report.signed_fake_gets,12);
 assert_eq!(report.verified_identical_scans,2);
 assert!(!report.full_lifetime_profit_proven
       &&!report.all_future_funding_between_open_and_close_proven
       &&!report.may_send_cancel_retry_or_verify_closed);
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2f2g_original_c6_partial_and_unknown_owner_zero_mock_get(){
 use spotarb_native_market_core::{
   native_trade2f_okx_owner_mock::{inspect_owner_bound_okx_offline_mock,F2GError},
 };
 let (dir,store,state,at)=trade2d2b_seed();
 // Empty mock response tape: only the PRE-GET owner DENY can be returned.
 // A tape read would fail with "mock response tape exhausted" instead.
 let out=inspect_owner_bound_okx_offline_mock(
    &store,state.generation,at,OkxDomain::Global,&[]);
 assert!(matches!(out,Err(F2GError::Deny(
    "four-leg original owner preflight denied before GET"))));
 std::fs::remove_dir_all(dir).unwrap();
}
