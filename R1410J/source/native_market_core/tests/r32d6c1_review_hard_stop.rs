//! R32D6C1 INTEGRATION tests: this library is compiled WITHOUT cfg(test).
//! All exchange responses are local mocks; no real network requests.
use spotarb_native_market_core::{
    authoritative_state::{NativeAuthoritativeState,NativeEntryPlan,NativeStateStore},
    execution_adapter::{OrderIntent,OrderKind,Venue},
    fill_economics::{decimal_to_atoms,PositionEconomicsState,EntryEconomicsState,EntryTiming,LegFillAggregate},
    native_hard_safety::{require_native_mutation_egress,NativeHardSafetyError},
    private_https::{
        dispatch_order_once,dispatch_authorized_entry_once,query_order_once,
        read_binance_position_mode_once,HttpsClient,HttpExecutor,HttpsRequest,
        HttpResponse,NativeEntryDispatchError,TransportError,
    },
    private_rest::{
        HttpMethod,SignedRequest,CredentialsRef,OkxCredentials,OkxDomain,
        BinancePositionMode,build_signed_entry_order_from_native_authority,
        RestOutcomeClass,BINANCE_DEFAULT_RECV_WINDOW_MS,
    },
};
use std::sync::atomic::{AtomicUsize,Ordering};

#[derive(Default)]
struct CountingExecutor{calls:AtomicUsize}
impl HttpExecutor for CountingExecutor{
    fn execute_request(&self,_req:&HttpsRequest)->Result<HttpResponse,TransportError>{
        self.calls.fetch_add(1,Ordering::SeqCst);
        Ok(HttpResponse{
            status:200,headers:vec![],
            body:r#"{"orderId":777,"clientOrderId":"EXACT","status":"FILLED","executedQty":"1"}"#.into(),
        })
    }
}
fn synthetic_signed_close()->SignedRequest{
    SignedRequest{
        venue:Venue::BinanceUsdm,method:HttpMethod::Post,
        url:"https://fapi.binance.com/fapi/v1/order".into(),
        headers:vec![("X-MBX-APIKEY".into(),"FAKE-NOT-REAL".into())],
        body:Some("symbol=BNTUSDT&side=BUY&type=MARKET&reduceOnly=true&quantity=1".into()),
        client_order_id:Some("EXACT".into()),is_mutation:true,
    }
}
fn temp_fixture()->std::path::PathBuf{
    let ns=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .unwrap().as_nanos();
    std::env::temp_dir().join(format!("r32d6c1-hardstop-{}-{ns}",std::process::id()))
}
fn economics()->PositionEconomicsState{
    let one=decimal_to_atoms("1").unwrap();
    PositionEconomicsState{
        entry:EntryEconomicsState{
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
        },exit:None,
    }
}
#[test]
fn r32d6c1_normal_linked_library_is_compile_time_mutation_denied(){
    assert_eq!(require_native_mutation_egress(),
        Err(NativeHardSafetyError::MutationCapabilityNotCertified));
}
#[test]
fn r32d6c1_public_legacy_close_and_fake_executor_cannot_send(){
    let fake=CountingExecutor::default();
    let obs=dispatch_order_once(&fake,&synthetic_signed_close());
    assert_eq!(obs.class,RestOutcomeClass::Unknown);
    assert_eq!(obs.reason,"NATIVE_HARD_SAFETY_ENGAGED");
    assert_eq!(fake.calls.load(Ordering::SeqCst),0);
}
#[test]
fn r32d6c1_read_helpers_reject_disguised_post_regardless_mutation_flag(){
    let fake=CountingExecutor::default();
    let mut bad=synthetic_signed_close();
    for mutation in [false,true]{
        bad.is_mutation=mutation;
        assert_eq!(query_order_once(&fake,&bad).reason,"NATIVE_HARD_SAFETY_ENGAGED");
        assert_eq!(read_binance_position_mode_once(&fake,&bad),BinancePositionMode::Unknown);
    }
    bad.method=HttpMethod::Get;bad.is_mutation=true;
    assert_eq!(query_order_once(&fake,&bad).reason,"NATIVE_HARD_SAFETY_ENGAGED");
    assert_eq!(read_binance_position_mode_once(&fake,&bad),BinancePositionMode::Unknown);
    assert_eq!(fake.calls.load(Ordering::SeqCst),0);
}
#[test]
fn r32d6c1_exact_readonly_query_is_not_disabled_for_mock(){
    let fake=CountingExecutor::default();
    let mut get=synthetic_signed_close();
    get.url="https://fapi.binance.com/fapi/v1/order?symbol=BNTUSDT&origClientOrderId=EXACT".into();
    get.method=HttpMethod::Get;get.is_mutation=false;get.body=None;
    let _=query_order_once(&fake,&get);
    assert_eq!(fake.calls.load(Ordering::SeqCst),1);
}
#[test]
fn r32d6c1_real_https_client_rejects_any_post_before_network(){
    let request=HttpsRequest::from_signed(&synthetic_signed_close());
    let err=HttpsClient::default().execute(&request).unwrap_err();
    assert_eq!(err.kind,spotarb_native_market_core::private_https::TransportErrorKind::EndpointDenied);
}
#[test]
fn r32d6c1_durable_exact_entry_prepared_still_denied_before_fake_http(){
    let root=temp_fixture();
    let store=NativeStateStore::open(root.join("state.json")).unwrap();
    store.create(&NativeAuthoritativeState::empty_from_legacy("11".repeat(32),0))
        .unwrap();
    let plan=NativeEntryPlan{
        base:"BNT".into(),spot_symbol:"BNT/USDT".into(),futures_symbol:"BNTUSDT".into(),
        created_at_ms:1_780_000_000_125,
        planned_qty_atoms_e18:1_000_000_000_000_000_000,
        order_quote_atoms_e18:10_000_000_000_000_000_000,
        spot_baseline_atoms_e18:125_000_000_000_000_000,
        safety_generation:7,
    };
    let cid="sab00000000000000000000000000751";
    let (saved,permit)=store.begin_native_entry(1,plan,economics(),"hard-stop-spot",cid).unwrap();
    let original=std::fs::read(root.join("state.json")).unwrap();
    let intent=OrderIntent{
        venue:Venue::OkxSpot,symbol:"BNT/USDT".into(),side:"buy".into(),
        amount:"1".into(),client_id:cid.into(),
    };
    let authorized=store.authorize_entry_order(permit,&intent,7).unwrap();
    let fake_creds=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
    let signed=build_signed_entry_order_from_native_authority(
        authorized,OrderKind::AggressiveLimitFok,Some("0.75"),
        CredentialsRef::Okx(&fake_creds),1_700_000_000_123,
        OkxDomain::Global,BinancePositionMode::OneWay,
        BINANCE_DEFAULT_RECV_WINDOW_MS,
    ).unwrap();
    let fake=CountingExecutor::default();
    assert!(matches!(
        dispatch_authorized_entry_once(&fake,&store,signed,7),
        Err(NativeEntryDispatchError::HardSafety(
            NativeHardSafetyError::MutationCapabilityNotCertified))
    ));
    assert_eq!(fake.calls.load(Ordering::SeqCst),0);
    assert_eq!(std::fs::read(root.join("state.json")).unwrap(),original);
    assert_eq!(store.load().unwrap().unwrap(),saved);
    let recovery=store.load_for_recovery().unwrap().unwrap();
    assert_eq!(recovery.entry.unwrap().phase,
        spotarb_native_market_core::authoritative_state::NativeEntryPhase::RecoveryRequired);
    std::fs::remove_dir_all(root).unwrap();
}
