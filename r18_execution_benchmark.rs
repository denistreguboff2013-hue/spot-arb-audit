use serde_json::json;
use spotarb_native_market_core::{
    Book,
    decision::{DecisionConfig, DecisionCore, Limits, RouteSpec},
    execution::{Coordinator, CoordinatorState, Leg},
    execution_adapter::{OrderIntent, OrderKind, Venue as ExecVenue},
    execution_store::DurableMutationGate,
    private_https::{dispatch_order_once, query_order_once, HttpExecutor, HttpResponse, HttpsRequest, TransportError},
    private_rest::{
        build_okx_query_by_client_id, build_signed_order_from_network_boundary,
        BinanceCredentials, BinancePositionMode, CredentialsRef, OkxCredentials, OkxDomain,
        RestOutcomeClass, BINANCE_DEFAULT_RECV_WINDOW_MS,
    },
    safety::{evaluate as evaluate_safety, SafetyInput},
};
use std::{
    collections::{HashMap, HashSet},
    fs,
    hint::black_box,
    path::PathBuf,
    time::Instant,
};

#[derive(Clone)]
struct StaticExecutor {
    response: HttpResponse,
}
impl HttpExecutor for StaticExecutor {
    fn execute_request(&self, _request: &HttpsRequest) -> Result<HttpResponse, TransportError> {
        Ok(self.response.clone())
    }
}

fn elapsed_ns(t: Instant) -> u64 {
    t.elapsed().as_nanos().min(u64::MAX as u128) as u64
}
fn percentile(v: &[u64], q: f64) -> f64 {
    let mut s=v.to_vec(); s.sort_unstable();
    let idx=((q*s.len() as f64).ceil() as usize).saturating_sub(1).min(s.len()-1);
    s[idx] as f64 / 1_000_000.0
}
fn summary(v: &[u64]) -> serde_json::Value {
    let mean=v.iter().map(|x|*x as f64).sum::<f64>()/v.len() as f64/1_000_000.0;
    json!({"n":v.len(),"p50_ms":percentile(v,0.50),"p95_ms":percentile(v,0.95),
        "p99_ms":percentile(v,0.99),"min_ms":*v.iter().min().unwrap() as f64/1e6,
        "max_ms":*v.iter().max().unwrap() as f64/1e6,"mean_ms":mean})
}

fn route() -> RouteSpec {
    RouteSpec {
        buy_exchange:"okx_spot".into(), sell_exchange:"binance_futures".into(),
        label:"OKX Spot BUY -> Binance Futures SHORT".into(),
        buy:Limits{min_amount:"1".into(),step:"0.1".into(),min_cost:"0".into(),max_amount:Some("100000".into())},
        sell:Limits{min_amount:"1".into(),step:"0.1".into(),min_cost:"0".into(),max_amount:Some("100000".into())},
        loaded_ns:1, metadata_state:"SIZE_METADATA_READY".into(),
    }
}
fn core() -> DecisionCore {
    let cfg=DecisionConfig{max_age_ms:800,metadata_ttl_ms:3_600_000,base_size:10.0,risk_cap:10.0,
        cost:24.0,required_net:42.0,min_price:0.0,max_price:f64::INFINITY,blacklist:HashSet::new()};
    let mut routes=HashMap::new(); routes.insert("TEST".into(),vec![route()]);
    DecisionCore::new(0,1,cfg,routes).unwrap()
}
fn book(price:f64, qty:f64, ns:u64, wall:i64, seq:i64) -> Book {
    Book{bids:vec![(price,qty)],asks:vec![(price,qty)],received_ns:ns,received_wall_ms:wall,
        exchange_ts_ms:Some(wall),changed_ns:ns,epoch:1,sequence:Some(seq)}
}
fn safety_input() -> SafetyInput {
    SafetyInput {
        trade_enabled:true,trial_enabled:true,operator_approved:true,max_trades_per_day:0,
        entry_cooldown_sec:10800,max_open_positions:1,deny_on_unknown:true,deny_on_untracked_assets:true,
        position_state_valid:true,active_position:false,paper_position:false,unknown_or_manual_state:false,
        preflight_present:true,preflight_path_ok:true,preflight_read_ok:true,preflight_size_ok:true,
        preflight_valid:true,preflight_max_age_ok:true,preflight_schema_ok:true,preflight_fresh:true,
        preflight_allow:true,preflight_has_blockers:false,exchange_polled:true,live_checks_complete:true,
        live_reconciliation_complete:true,outbox_unresolved:0,spot_inventory_policy_valid:true,
        inventory_policy_match:true,untracked_spot:false,untracked_futures:false,untracked_orders:false,
        untracked_exchange_assets:false,lifecycle_verified:true,bootstrap_policy_valid:true,
        bootstrap_valid:true,prior_incident_valid:true,cooldown_evidence_present:true,
        cooldown_state_present:true,cooldown_evidence_valid:true,cooldown_remaining_sec:0,
        auto_preflight_refresh_failed:false,kill_path_ok:true,kill_read_ok:true,kill_size_ok:true,
        kill_payload_ok:true,kill_switch_present:true,kill_switch_schema_ok:true,
        kill_switch_operator_ok:true,kill_switch_state:"DISENGAGED".into(),kill_drill_age_ok:true,
        kill_drilled:true,kill_switch_drill_fresh:true,kill_max_age_ok:true,
        kill_switch_disengagement_fresh:true,
    }
}
fn arg(name:&str)->String{
    let a:Vec<String>=std::env::args().collect();
    let i=a.iter().position(|x|x==name).unwrap_or_else(||panic!("missing {name}"));
    a.get(i+1).unwrap().clone()
}

fn main() {
    let iterations:usize=arg("--iterations").parse().unwrap();
    let workdir=PathBuf::from(arg("--workdir"));
    let run_id:u64=arg("--run-id").parse().unwrap();
    let output=PathBuf::from(arg("--output"));
    let root=workdir.join(format!("rust-{run_id}"));
    if root.exists(){fs::remove_dir_all(&root).unwrap();}
    fs::create_dir_all(&root).unwrap();
    let intent_path=root.join("intents.json");
    let mut gate=DurableMutationGate::open(&intent_path).unwrap();
    let mut decision_core=core();
    let safety_fixture=safety_input();
    let okx=OkxCredentials::new("R18DUMMYOKXKEY","R18DUMMYOKXSECRET","R18DUMMYOKXPASS").unwrap();
    let binance=BinanceCredentials::new("R18DUMMYBINKEY","R18DUMMYBINSECRET").unwrap();

    let okx_place=StaticExecutor{response:HttpResponse{status:200,headers:vec![],
        body:r#"{"code":"0","data":[{"clOrdId":"x","ordId":"1001","sCode":"0","sMsg":""}],"msg":""}"#.into()}};
    let okx_query=StaticExecutor{response:HttpResponse{status:200,headers:vec![],
        body:r#"{"code":"0","data":[{"clOrdId":"x","ordId":"1001","state":"filled","accFillSz":"10"}],"msg":""}"#.into()}};
    let bin_result=StaticExecutor{response:HttpResponse{status:200,headers:vec![],
        body:r#"{"orderId":2001,"clientOrderId":"x","status":"FILLED","executedQty":"10"}"#.into()}};

    let names=["decision","safety","spot_prepare","spot_sign","spot_transport_classify","spot_ack",
        "spot_query_sign","spot_query_transport_classify","futures_prepare","futures_sign",
        "futures_transport_classify","futures_ack","durable_total","sign_total",
        "transport_classify_total","total"];
    let mut st:HashMap<&str,Vec<u64>>=names.into_iter().map(|x|(x,Vec::with_capacity(iterations))).collect();
    let mut semantic=json!({});

    for i in 0..iterations {
        let seq=run_id*100_000+(i as u64)*2+1;
        let spot_cid=format!("sab{seq:029}");
        let fut_cid=format!("sab{:029}",seq+1);
        let ns=1_000_000_000+(i as u64)*1_000_000;
        let wall=1_700_000_000_000i64+i as i64;
        let total=Instant::now();

        // Spot side is already available when the decision-triggering futures update arrives.
        let _=decision_core.update_at("TEST","okx_spot",book(1.0,1000.0,ns,wall,(i*2+1) as i64),ns,wall).unwrap();
        let t=Instant::now();
        let decisions=decision_core.update_at("TEST","binance_futures",book(1.007,1000.0,ns+1,wall,(i*2+2) as i64),ns+1,wall).unwrap();
        let decision_ns=elapsed_ns(t); st.get_mut("decision").unwrap().push(decision_ns);
        let d=&decisions[0]; assert_eq!(d.reason,"EXECUTABLE_OPPORTUNITY"); black_box(d);

        let t=Instant::now(); let s=evaluate_safety(&safety_fixture); let safety_ns=elapsed_ns(t);
        st.get_mut("safety").unwrap().push(safety_ns); assert!(s.allowed); assert!(!s.real_launch_authorized); black_box(&s);

        let mut coord=Coordinator::new(7);
        let spot_id=format!("r18-rs-{run_id}-{i}-spot");
        let spot_intent=OrderIntent{venue:ExecVenue::OkxSpot,symbol:"TEST/USDT".into(),side:"buy".into(),amount:"10".into(),client_id:spot_cid.clone()};
        let t=Instant::now();
        coord.prepare_spot(7).unwrap(); gate.begin(&spot_id,Leg::Spot,&spot_cid).unwrap();
        let spot_action=gate.after_durable_prepare(&spot_id).unwrap();
        let spot_prepare=elapsed_ns(t); st.get_mut("spot_prepare").unwrap().push(spot_prepare);

        let t=Instant::now();
        let spot_signed=build_signed_order_from_network_boundary(&spot_action,&spot_intent,OrderKind::AggressiveLimitFok,false,Some("1"),7,7,
            CredentialsRef::Okx(&okx),1_700_000_000_123,OkxDomain::Global,BinancePositionMode::OneWay,BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        let spot_sign=elapsed_ns(t); st.get_mut("spot_sign").unwrap().push(spot_sign); black_box(&spot_signed);

        let t=Instant::now(); let spot_obs=dispatch_order_once(&okx_place,&spot_signed); let spot_transport=elapsed_ns(t);
        st.get_mut("spot_transport_classify").unwrap().push(spot_transport);
        assert_eq!(spot_obs.class,RestOutcomeClass::Acknowledged);

        let t=Instant::now(); gate.acknowledge(&spot_id,false).unwrap(); let spot_ack=elapsed_ns(t);
        st.get_mut("spot_ack").unwrap().push(spot_ack);

        let t=Instant::now(); let spot_query_req=build_okx_query_by_client_id(&okx,1_700_000_000_123,OkxDomain::Global,"TEST/USDT",&spot_cid).unwrap();
        let spot_query_sign=elapsed_ns(t); st.get_mut("spot_query_sign").unwrap().push(spot_query_sign);
        let t=Instant::now(); let spot_truth=query_order_once(&okx_query,&spot_query_req); let spot_query_transport=elapsed_ns(t);
        st.get_mut("spot_query_transport_classify").unwrap().push(spot_query_transport);
        assert_eq!(spot_truth.class,RestOutcomeClass::Acknowledged); assert!(spot_truth.terminal); assert_eq!(spot_truth.filled.as_deref(),Some("10"));
        coord.spot_result(10,10,false).unwrap();

        let fut_id=format!("r18-rs-{run_id}-{i}-fut");
        let fut_intent=OrderIntent{venue:ExecVenue::BinanceUsdm,symbol:"TEST/USDT:USDT".into(),side:"sell".into(),amount:"10".into(),client_id:fut_cid.clone()};
        let t=Instant::now();
        coord.prepare_futures(7).unwrap(); gate.begin(&fut_id,Leg::Futures,&fut_cid).unwrap();
        let fut_action=gate.after_durable_prepare(&fut_id).unwrap();
        let fut_prepare=elapsed_ns(t); st.get_mut("futures_prepare").unwrap().push(fut_prepare);

        let t=Instant::now();
        let fut_signed=build_signed_order_from_network_boundary(&fut_action,&fut_intent,OrderKind::Market,false,None,7,7,
            CredentialsRef::Binance(&binance),1_700_000_000_123,OkxDomain::Global,BinancePositionMode::OneWay,BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        let fut_sign=elapsed_ns(t); st.get_mut("futures_sign").unwrap().push(fut_sign); black_box(&fut_signed);

        let t=Instant::now(); let fut_obs=dispatch_order_once(&bin_result,&fut_signed); let fut_transport=elapsed_ns(t);
        st.get_mut("futures_transport_classify").unwrap().push(fut_transport);
        assert_eq!(fut_obs.class,RestOutcomeClass::Acknowledged); assert!(fut_obs.terminal); assert_eq!(fut_obs.filled.as_deref(),Some("10"));

        let t=Instant::now(); gate.acknowledge(&fut_id,false).unwrap(); coord.futures_result(10,10,false).unwrap(); let fut_ack=elapsed_ns(t);
        st.get_mut("futures_ack").unwrap().push(fut_ack);
        assert_eq!(coord.state,CoordinatorState::Hedged);

        st.get_mut("durable_total").unwrap().push(spot_prepare+spot_ack+fut_prepare+fut_ack);
        st.get_mut("sign_total").unwrap().push(spot_sign+spot_query_sign+fut_sign);
        st.get_mut("transport_classify_total").unwrap().push(spot_transport+spot_query_transport+fut_transport);
        st.get_mut("total").unwrap().push(elapsed_ns(total));
        semantic=json!({"decision_reason":d.reason,"safety_allowed":s.allowed,"safety_blocker_codes":s.blocker_codes,"preflight_is_not_launch_authority":s.preflight_is_not_launch_authority,"real_launch_authorized":s.real_launch_authorized,
            "spot_status":"closed","spot_filled":10.0,"futures_status":"filled","futures_filled":10.0,"final_state":"HEDGED"});
    }

    let mut stage_json=serde_json::Map::new();
    for name in names {stage_json.insert(name.into(),summary(&st[name]));}
    let bytes=fs::metadata(&intent_path).unwrap().len();
    let result=json!({"engine":"rust","run_id":run_id,"iterations":iterations,"build":"release",
        "durability":"R14 DurableIntentStore temp+file fsync+rename+parent dir fsync","workdir":root,
        "intent_store_bytes":bytes,"semantic":semantic,"stages":stage_json});
    fs::write(output,serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    println!("{}",serde_json::to_string(&result).unwrap());
}
