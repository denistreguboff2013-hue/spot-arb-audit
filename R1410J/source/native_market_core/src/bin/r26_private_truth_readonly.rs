use spotarb_native_market_core::{
    private_https::{fetch_public_time, wall_ms, HttpExecutor, HttpsRequest, PublicTimeVenue, ReadOnlyHttpsClient,
        DEFAULT_MAX_TIME_SAMPLE_RTT_MS, DEFAULT_TIME_SAMPLE_MAX_AGE_MS},
    private_rest::{
        build_binance_account_config_request, build_binance_balance_v3_request,
        build_binance_open_orders_request, build_binance_position_mode_request,
        build_binance_position_risk_v3_request, build_okx_account_config_request,
        build_okx_balance_request, build_okx_pending_spot_orders_request,
        build_okx_positions_request, parse_binance_position_mode_http,
        BinanceCredentials, BinancePositionMode, OkxCredentials, OkxDomain, SignedRequest,
        BINANCE_DEFAULT_RECV_WINDOW_MS,
    },
    private_truth::{
        parse_binance_account_config, parse_binance_balances, parse_binance_open_orders,
        parse_binance_positions, parse_okx_account_config, parse_okx_balances,
        parse_okx_pending_orders, parse_okx_positions, summarize,
    },
};
use serde_json::json;
use std::{env, fs, path::PathBuf};

fn required(name:&str, fallback:&str)->Result<String,String>{
    env::var(name).or_else(|_|env::var(fallback)).map_err(|_|format!("missing required environment variable {name}"))
}
fn okx_domain()->Result<OkxDomain,String>{
    match env::var("R26_OKX_DOMAIN").unwrap_or_else(|_|"global".into()).to_ascii_lowercase().as_str(){
        "global"=>Ok(OkxDomain::Global),"eea"=>Ok(OkxDomain::Eea),"us"=>Ok(OkxDomain::Us),
        "openapi"=>Ok(OkxDomain::OpenApi),_=>Err("invalid R26_OKX_DOMAIN".into()),
    }
}
fn execute(client:&ReadOnlyHttpsClient,req:&SignedRequest)->Result<spotarb_native_market_core::private_https::HttpResponse,String>{
    let h=HttpsRequest::from_signed(req);
    if h.mutation{return Err("R26 invariant: mutation request constructed".into())}
    client.execute_request(&h).map_err(|e|format!("transport:{e:?}"))
}
fn corrected_time(client:&ReadOnlyHttpsClient,venue:PublicTimeVenue)->Result<u64,String>{
    let sample=fetch_public_time(client,venue).map_err(|e|format!("time:{e:?}"))?;
    let now=wall_ms();
    if !sample.usable_at(now,DEFAULT_TIME_SAMPLE_MAX_AGE_MS,DEFAULT_MAX_TIME_SAMPLE_RTT_MS){
        return Err("time sample unusable".into())
    }
    sample.corrected_ms(now).ok_or_else(||"corrected time overflow".into())
}

fn main()->Result<(),String>{
    if env::var("R26_LIVE_READONLY_AUTHORIZED").ok().as_deref()!=Some("YES"){
        return Err("explicit read-only live authorization missing".into())
    }
    // Credentials are read only after the explicit live-read authorization gate.
    let bn=BinanceCredentials::new(&required("R26_BINANCE_API_KEY","BINANCE_KEY")?,&required("R26_BINANCE_SECRET","BINANCE_SECRET")?)
        .map_err(|e|format!("binance credentials:{e:?}"))?;
    let ok=OkxCredentials::new(&required("R26_OKX_API_KEY","OKX_KEY")?,&required("R26_OKX_SECRET","OKX_SECRET")?,&required("R26_OKX_PASSPHRASE","OKX_PASSPHRASE")?)
        .map_err(|e|format!("okx credentials:{e:?}"))?;
    let domain=okx_domain()?;
    let client=ReadOnlyHttpsClient::default();

    let bt=corrected_time(&client,PublicTimeVenue::BinanceUsdm)?;
    let mode_req=build_binance_position_mode_request(&bn,bt,BINANCE_DEFAULT_RECV_WINDOW_MS).map_err(|e|format!("{e:?}"))?;
    let mode_http=execute(&client,&mode_req)?;
    let mode=parse_binance_position_mode_http(mode_http.status,&mode_http.body);
    if mode==BinancePositionMode::Unknown{return Err("binance position mode unknown".into())}
    let bc_http=execute(&client,&build_binance_account_config_request(&bn,bt,BINANCE_DEFAULT_RECV_WINDOW_MS).map_err(|e|format!("{e:?}"))?)?;
    let bb_http=execute(&client,&build_binance_balance_v3_request(&bn,bt,BINANCE_DEFAULT_RECV_WINDOW_MS).map_err(|e|format!("{e:?}"))?)?;
    let bp_http=execute(&client,&build_binance_position_risk_v3_request(&bn,bt,BINANCE_DEFAULT_RECV_WINDOW_MS).map_err(|e|format!("{e:?}"))?)?;
    let bo_http=execute(&client,&build_binance_open_orders_request(&bn,bt,BINANCE_DEFAULT_RECV_WINDOW_MS).map_err(|e|format!("{e:?}"))?)?;
    let bc=parse_binance_account_config(bc_http.status,&bc_http.body).map_err(|e|format!("binance account config:{e:?}"))?;
    let bb=parse_binance_balances(bb_http.status,&bb_http.body).map_err(|e|format!("binance balances:{e:?}"))?;
    let bp=parse_binance_positions(bp_http.status,&bp_http.body).map_err(|e|format!("binance positions:{e:?}"))?;
    let bo=parse_binance_open_orders(bo_http.status,&bo_http.body).map_err(|e|format!("binance open orders:{e:?}"))?;

    let ot=corrected_time(&client,PublicTimeVenue::Okx(domain))?;
    let oc_http=execute(&client,&build_okx_account_config_request(&ok,ot,domain).map_err(|e|format!("{e:?}"))?)?;
    let ob_http=execute(&client,&build_okx_balance_request(&ok,ot,domain).map_err(|e|format!("{e:?}"))?)?;
    let op_http=execute(&client,&build_okx_positions_request(&ok,ot,domain).map_err(|e|format!("{e:?}"))?)?;
    let oo_http=execute(&client,&build_okx_pending_spot_orders_request(&ok,ot,domain).map_err(|e|format!("{e:?}"))?)?;
    if !(200..300).contains(&oc_http.status) {
        let v:serde_json::Value=serde_json::from_str(&oc_http.body).unwrap_or(serde_json::Value::Null);
        let code=v.get("code").and_then(serde_json::Value::as_str).unwrap_or("unknown");
        let msg=v.get("msg").and_then(serde_json::Value::as_str).unwrap_or("unknown");
        return Err(format!("okx account config http={} code={} msg={}",oc_http.status,code,msg));
    }
    let oc=parse_okx_account_config(oc_http.status,&oc_http.body).map_err(|e|format!("okx account config:{e:?}"))?;
    let ob=parse_okx_balances(ob_http.status,&ob_http.body).map_err(|e|format!("okx balances:{e:?}"))?;
    let op=parse_okx_positions(op_http.status,&op_http.body).map_err(|e|format!("okx positions:{e:?}"))?;
    let oo=parse_okx_pending_orders(oo_http.status,&oo_http.body).map_err(|e|format!("okx pending orders:{e:?}"))?;

    let summary=summarize(mode,&bc,&bb,&bp,&bo,&oc,&ob,&op,&oo).map_err(|e|format!("summary:{e:?}"))?;
    let out=json!({
        "r26_private_truth_readonly":true,
        "read_only_transport":true,
        "network_mutation_possible":false,
        "queries_complete":true,
        "summary":summary,
        "detail":{
            "binance_balances":bb,"binance_positions":bp,"binance_open_orders":bo,
            "okx_balances":ob,"okx_positions":op,"okx_pending_spot_orders":oo
        }
    });
    if let Some(path)=env::var_os("R26_OUTPUT").map(PathBuf::from){
        fs::write(path,serde_json::to_vec_pretty(&out).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
    }
    println!("{}",serde_json::to_string(&out).map_err(|e|e.to_string())?);
    Ok(())
}
