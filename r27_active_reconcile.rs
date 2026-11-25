use serde_json::{json, Map, Value};
use std::{collections::{BTreeMap,BTreeSet}, env, fs, path::Path};

const SCALE_DIGITS:i32=18;
#[cfg(test)]
const SCALE:i128=1_000_000_000_000_000_000;
const QTY_TOLERANCE_BPS:i128=200;

fn norm(s:&str)->String{s.trim().to_ascii_uppercase()}
fn pow10_i128(n:u32)->Result<i128,String>{
    let mut v=1i128;
    for _ in 0..n{v=v.checked_mul(10).ok_or("decimal overflow")?;}
    Ok(v)
}
fn parse_dec(s:&str)->Result<i128,String>{
    let t=s.trim(); if t.is_empty(){return Err("empty decimal".into())}
    let (neg,u)=if let Some(x)=t.strip_prefix('-'){(true,x)}
        else if let Some(x)=t.strip_prefix('+'){(false,x)} else{(false,t)};
    if u.is_empty(){return Err("invalid decimal".into())}
    let mut exp_split=u.split(|c|c=='e'||c=='E');
    let mant=exp_split.next().unwrap_or("");
    let exp_txt=exp_split.next();
    if exp_split.next().is_some(){return Err("invalid decimal".into())}
    let exp:i32=match exp_txt{
        Some(x) if !x.is_empty()=>x.parse().map_err(|_|"invalid exponent".to_string())?,
        Some(_)=>return Err("invalid exponent".into()),
        None=>0,
    };
    if !(-36..=36).contains(&exp){return Err("exponent out of range".into())}
    let mut parts=mant.split('.');
    let whole=parts.next().unwrap_or("");let frac=parts.next().unwrap_or("");
    if parts.next().is_some()||whole.is_empty()||
        !whole.bytes().all(|b|b.is_ascii_digit())||!frac.bytes().all(|b|b.is_ascii_digit()){
        return Err("invalid decimal".into())
    }
    let digits_txt=format!("{whole}{frac}");
    let digits:i128=digits_txt.parse().map_err(|_|"decimal overflow".to_string())?;
    let power=SCALE_DIGITS+exp-frac.len() as i32;
    let scaled=if power>=0{
        digits.checked_mul(pow10_i128(power as u32)?).ok_or("decimal overflow")?
    }else{
        let d=pow10_i128((-power) as u32)?;
        if digits%d!=0{return Err("precision exceeds fixed decimal scale".into())}
        digits/d
    };
    Ok(if neg{-scaled}else{scaled})
}
fn value_dec(v:&Value)->Result<i128,String>{
    if let Some(s)=v.as_str(){parse_dec(s)}
    else if v.is_number(){parse_dec(&v.to_string())}
    else{Err("quantity is not decimal".into())}
}
fn close_qty(actual:i128,expected:i128)->bool{
    if expected<=0{return false}
    let diff=(actual-expected).abs();
    diff.saturating_mul(10_000)<=expected.saturating_mul(QTY_TOLERANCE_BPS)
}
fn base_from_symbol(s:&str)->String{
    let u=norm(s); if u.contains('-'){return u.split('-').next().unwrap_or("").into()}
    for q in ["USDT","USDC","BUSD","FDUSD","USD"]{
        if u.ends_with(q)&&u.len()>q.len(){return u[..u.len()-q.len()].into()}
    }
    u
}
fn active_status(s:&str)->bool{
    matches!(s.to_ascii_lowercase().as_str(),
        "open"|"opened"|"opening"|"closing"|"error"|"unknown"|"verify_warning"|
        "orphan_spot"|"orphan_fut"|"orphan_spot_confirmed"|"orphan_fut_confirmed"|"manual_required")
}

fn live_mode(pos:&Map<String,Value>)->String{
    pos.get("_meta").and_then(Value::as_object).and_then(|m|m.get("open_mode"))
        .and_then(Value::as_str)
        .or_else(||pos.get("entry").and_then(Value::as_object).and_then(|m|m.get("mode")).and_then(Value::as_str))
        .unwrap_or("").to_ascii_lowercase()
}
fn okx_spot_balances(snapshot:&Value)->Result<BTreeMap<String,i128>,String>{
    let rows=snapshot.get("detail").and_then(|x|x.get("okx_balances")).and_then(Value::as_array)
        .ok_or("okx balances missing")?;
    let mut out=BTreeMap::new();
    for row in rows{
        let ccy=norm(row.get("currency").and_then(Value::as_str).ok_or("okx currency missing")?);
        let amount=value_dec(row.get("cash_balance").ok_or("okx cash_balance missing")?)?;
        if out.insert(ccy,amount).is_some(){return Err("duplicate OKX balance currency".into())}
    }
    Ok(out)
}
fn binance_futures(snapshot:&Value)->Result<BTreeMap<String,i128>,String>{
    let rows=snapshot.get("detail").and_then(|x|x.get("binance_positions")).and_then(Value::as_array)
        .ok_or("binance positions missing")?;
    let mut out=BTreeMap::new();
    for row in rows{
        let symbol=row.get("symbol").and_then(Value::as_str).ok_or("binance symbol missing")?;
        let amount=value_dec(row.get("position_amt").ok_or("binance position_amt missing")?)?;
        if amount==0{continue}
        let base=base_from_symbol(symbol);
        if out.insert(base,amount).is_some(){return Err("multiple Binance positions for base".into())}
    }
    Ok(out)
}

fn classify(base:&str,pos:&Map<String,Value>,spot_now:i128,fut_now:i128)->Value{
    let prior_status=pos.get("status").and_then(Value::as_str).unwrap_or("unknown");
    let baseline=pos.get("spot_baseline_amount").map(value_dec).transpose();
    let expected_spot=pos.get("spot_net_amount").map(value_dec).transpose();
    let expected_fut=pos.get("fut").and_then(Value::as_object)
        .and_then(|x|x.get("amount")).map(value_dec).transpose();
    if baseline.is_err()||expected_spot.is_err()||expected_fut.is_err()||
        baseline.as_ref().ok().and_then(|x|*x).is_none()||
        expected_spot.as_ref().ok().and_then(|x|*x).is_none()||
        expected_fut.as_ref().ok().and_then(|x|*x).is_none(){
        return json!({"base":base,"verdict":"DENY","truth":"STATE_EVIDENCE_MISSING",
            "manual_required":true,"prior_status":prior_status});
    }
    let baseline=baseline.unwrap().unwrap(); let expected_spot=expected_spot.unwrap().unwrap();
    let expected_fut=expected_fut.unwrap().unwrap();
    if expected_spot<=0||expected_fut<=0||baseline<0{
        return json!({"base":base,"verdict":"DENY","truth":"STATE_EVIDENCE_INVALID",
            "manual_required":true,"prior_status":prior_status});
    }
    let spot_net=spot_now-baseline;
    let has_spot=spot_net>0;
    let has_fut=fut_now!=0;

    let (truth,manual)=match (has_spot,has_fut){
        (true,true)=>{
            if fut_now>=0 {("FUTURES_DIRECTION_MISMATCH",true)}
            else if !close_qty(spot_net,expected_spot)||!close_qty(-fut_now,expected_fut) {
                ("QUANTITY_MISMATCH",true)
            } else {("HEDGED_VERIFIED",false)}
        }
        (true,false)=>("ORPHAN_SPOT",true),
        (false,true)=>("ORPHAN_FUTURES",true),
        (false,false)=>{
            if prior_status.eq_ignore_ascii_case("closing"){("CLOSED_REQUIRES_EXIT_TRUTH",true)}
            else{("UNVERIFIED_CLOSED",true)}
        }
    };
    json!({
        "base":base,
        "verdict":if manual{"DENY"}else{"PASS"},
        "truth":truth,
        "manual_required":manual,
        "prior_status":prior_status,
        "spot_expected_net_atoms":expected_spot,
        "spot_observed_net_atoms":spot_net,
        "futures_expected_short_atoms":expected_fut,
        "futures_observed_signed_atoms":fut_now,
        "tolerance_bps":QTY_TOLERANCE_BPS
    })
}

fn run(snapshot_path:&Path,state_path:&Path)->Result<Value,String>{
    let snapshot:Value=serde_json::from_str(&fs::read_to_string(snapshot_path).map_err(|e|e.to_string())?)
        .map_err(|e|format!("snapshot json:{e}"))?;
    if snapshot.get("r26_private_truth_readonly").and_then(Value::as_bool)!=Some(true)||
       snapshot.get("queries_complete").and_then(Value::as_bool)!=Some(true)||
       snapshot.get("network_mutation_possible").and_then(Value::as_bool)!=Some(false){
        return Err("private truth snapshot not certified".into())
    }
    if snapshot.get("summary").and_then(|x|x.get("binance_position_mode")).and_then(Value::as_str)!=Some("OneWay"){
        return Err("Binance OneWay not proven".into())
    }
    let state:Value=serde_json::from_str(&fs::read_to_string(state_path).map_err(|e|e.to_string())?)
        .map_err(|e|format!("state json:{e}"))?;
    let positions=state.get("positions").and_then(Value::as_object).ok_or("state positions missing")?;
    let spot=okx_spot_balances(&snapshot)?;
    let fut=binance_futures(&snapshot)?;
    let mut rows=Vec::new();
    let mut active_bases=BTreeSet::new();
    for (base_raw,v) in positions{
        let pos=v.as_object().ok_or("position is not object")?;
        if live_mode(pos)=="paper"{continue}
        let status=pos.get("status").and_then(Value::as_str).unwrap_or("");
        if !active_status(status){continue}
        let base=norm(base_raw);
        active_bases.insert(base.clone());
        let spot_now=*spot.get(&base).unwrap_or(&0);
        let fut_now=*fut.get(&base).unwrap_or(&0);
        rows.push(classify(&base,pos,spot_now,fut_now));
    }
    let untracked_futures:Vec<String>=fut.keys()
        .filter(|base|!active_bases.contains(*base)).cloned().collect();
    let no_active_positions=rows.is_empty();
    let rows_pass=rows.iter().all(|x|x.get("verdict").and_then(Value::as_str)==Some("PASS"));
    let pass=!no_active_positions&&rows_pass&&untracked_futures.is_empty();
    let safe_noop=no_active_positions&&untracked_futures.is_empty();
    let verdict=if pass{"PASS"}else if safe_noop{"NO_ACTIVE_POSITIONS"}else{"DENY"};
    Ok(json!({
        "r27_active_reconciliation":pass,
        "safe_noop":safe_noop,
        "verdict":verdict,
        "active_positions_checked":rows.len(),
        "untracked_futures":untracked_futures,
        "positions":rows,
        "auto_mutation_allowed":false
    }))
}

fn main()->Result<(),String>{
    let args:Vec<String>=env::args().collect();
    if args.len()!=3{return Err("usage: r27_active_reconcile SNAPSHOT STATE".into())}
    let result=run(Path::new(&args[1]),Path::new(&args[2]))?;
    println!("{}",serde_json::to_string_pretty(&result).map_err(|e|e.to_string())?);
    let pass=result.get("r27_active_reconciliation").and_then(Value::as_bool)==Some(true);
    let safe_noop=result.get("safe_noop").and_then(Value::as_bool)==Some(true);
    if pass||safe_noop{Ok(())}else{Err("R27 active reconciliation denied".into())}
}

#[cfg(test)]
mod tests{
    use super::*;
    use std::{fs,process};

    fn files(tag:&str,snapshot:&str,state:&str)->(std::path::PathBuf,std::path::PathBuf){
        let root=env::temp_dir().join(format!("r27rec-{}-{}-{}",process::id(),tag,
            std::thread::current().name().unwrap_or("t")));
        let _=fs::remove_dir_all(&root);fs::create_dir_all(&root).unwrap();
        let a=root.join("snapshot.json");let b=root.join("state.json");
        fs::write(&a,snapshot).unwrap();fs::write(&b,state).unwrap();(a,b)
    }
    fn snapshot(spot:&str,fut:&str)->String{
        format!(r#"{{"r26_private_truth_readonly":true,"queries_complete":true,
        "network_mutation_possible":false,"summary":{{"binance_position_mode":"OneWay"}},
        "detail":{{"okx_balances":{spot},"binance_positions":{fut}}}}}"#)
    }
    fn state(status:&str,base:&str,baseline:&str,spot_net:&str,fut_amt:&str)->String{
        format!(r#"{{"positions":{{"{base}":{{"status":"{status}","spot_baseline_amount":{baseline},
        "spot_net_amount":{spot_net},"spot":{{"amount":{spot_net}}},"fut":{{"amount":{fut_amt}}},
        "_meta":{{"open_mode":"live"}}}}}}}}"#)
    }

    #[test]
    fn verified_hedge_passes(){
        let s=snapshot(r#"[{"currency":"BNT","cash_balance":"1.50"}]"#,
            r#"[{"symbol":"BNTUSDT","position_amt":"-1.00"}]"#);
        let st=state("opened","BNT","0.50","1.00","1.00");
        let (a,b)=files("ok",&s,&st);let r=run(&a,&b).unwrap();
        assert_eq!(r["verdict"],"PASS");
        assert_eq!(r["positions"][0]["truth"],"HEDGED_VERIFIED");
    }

    #[test]
    fn orphan_spot_denies(){
        let s=snapshot(r#"[{"currency":"BNT","cash_balance":"1.50"}]"#,"[]");
        let st=state("opened","BNT","0.50","1.00","1.00");
        let (a,b)=files("ospot",&s,&st);let r=run(&a,&b).unwrap();
        assert_eq!(r["positions"][0]["truth"],"ORPHAN_SPOT");
        assert_eq!(r["verdict"],"DENY");
    }

    #[test]
    fn orphan_futures_denies(){
        let s=snapshot(r#"[{"currency":"BNT","cash_balance":"0.50"}]"#,
            r#"[{"symbol":"BNTUSDT","position_amt":"-1.00"}]"#);
        let st=state("opened","BNT","0.50","1.00","1.00");
        let (a,b)=files("ofut",&s,&st);let r=run(&a,&b).unwrap();
        assert_eq!(r["positions"][0]["truth"],"ORPHAN_FUTURES");
        assert_eq!(r["verdict"],"DENY");
    }

    #[test]
    fn futures_direction_mismatch_denies(){
        let s=snapshot(r#"[{"currency":"BNT","cash_balance":"1.50"}]"#,
            r#"[{"symbol":"BNTUSDT","position_amt":"1.00"}]"#);
        let st=state("opened","BNT","0.50","1.00","1.00");
        let (a,b)=files("dir",&s,&st);let r=run(&a,&b).unwrap();
        assert_eq!(r["positions"][0]["truth"],"FUTURES_DIRECTION_MISMATCH");
        assert_eq!(r["verdict"],"DENY");
    }

    #[test]
    fn quantity_mismatch_denies(){
        let s=snapshot(r#"[{"currency":"BNT","cash_balance":"1.20"}]"#,
            r#"[{"symbol":"BNTUSDT","position_amt":"-1.00"}]"#);
        let st=state("opened","BNT","0.50","1.00","1.00");
        let (a,b)=files("qty",&s,&st);let r=run(&a,&b).unwrap();
        assert_eq!(r["positions"][0]["truth"],"QUANTITY_MISMATCH");
        assert_eq!(r["verdict"],"DENY");
    }

    #[test]
    fn two_percent_tolerance_matches_entry_contract(){
        let s=snapshot(r#"[{"currency":"BNT","cash_balance":"1.48"}]"#,
            r#"[{"symbol":"BNTUSDT","position_amt":"-0.98"}]"#);
        let st=state("opened","BNT","0.50","1.00","1.00");
        let (a,b)=files("tol",&s,&st);let r=run(&a,&b).unwrap();
        assert_eq!(r["positions"][0]["truth"],"HEDGED_VERIFIED");
        assert_eq!(r["verdict"],"PASS");
    }

    #[test]
    fn missing_state_evidence_denies(){
        let s=snapshot(r#"[{"currency":"BNT","cash_balance":"1"}]"#,
            r#"[{"symbol":"BNTUSDT","position_amt":"-1"}]"#);
        let st=r#"{"positions":{"BNT":{"status":"opened","_meta":{"open_mode":"live"}}}}"#;
        let (a,b)=files("missing",&s,st);let r=run(&a,&b).unwrap();
        assert_eq!(r["positions"][0]["truth"],"STATE_EVIDENCE_MISSING");
        assert_eq!(r["verdict"],"DENY");
    }

    #[test]
    fn exchange_flat_without_exit_truth_denies(){
        let s=snapshot(r#"[{"currency":"BNT","cash_balance":"0.50"}]"#,"[]");
        let st=state("opened","BNT","0.50","1.00","1.00");
        let (a,b)=files("closed",&s,&st);let r=run(&a,&b).unwrap();
        assert_eq!(r["positions"][0]["truth"],"UNVERIFIED_CLOSED");
        assert_eq!(r["verdict"],"DENY");
    }

    #[test]
    fn paper_positions_are_out_of_live_reconcile_scope(){
        let s=snapshot("[]","[]");
        let st=r#"{"positions":{"BNT":{"status":"opened","_meta":{"open_mode":"paper"}}}}"#;
        let (a,b)=files("paper",&s,st);let r=run(&a,&b).unwrap();
        assert_eq!(r["active_positions_checked"],0);
        assert_eq!(r["verdict"],"NO_ACTIVE_POSITIONS");
        assert_eq!(r["safe_noop"],true);
        assert_eq!(r["r27_active_reconciliation"],false);
    }

    #[test]
    fn untracked_futures_outside_state_denies(){
        let s=snapshot("[]",r#"[{"symbol":"ETHUSDT","position_amt":"-0.1"}]"#);
        let st=r#"{"positions":{}}"#;
        let (a,b)=files("untracked",&s,st);let r=run(&a,&b).unwrap();
        assert_eq!(r["verdict"],"DENY");
        assert_eq!(r["untracked_futures"][0],"ETH");
    }

    #[test]
    fn scientific_notation_is_parsed_exactly(){
        assert_eq!(parse_dec("1.23e-5").unwrap(),12_300_000_000_000);
        assert_eq!(parse_dec("-2E-3").unwrap(),-2_000_000_000_000_000);
        assert_eq!(parse_dec("5e2").unwrap(),500*SCALE);
        assert!(parse_dec("1e-19").is_err());
    }
}
