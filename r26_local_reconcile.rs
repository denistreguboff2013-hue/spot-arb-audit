use serde_json::{json, Value};
use std::{collections::HashSet, env, fs, path::Path};

fn norm(s:&str)->String{s.trim().to_ascii_uppercase()}
fn quote_asset(s:&str)->bool{matches!(norm(s).as_str(),"USDT"|"USDC"|"USD"|"FDUSD"|"BUSD")}
fn decimal_nonzero(s:&str)->Result<bool,String>{
    let t=s.trim().trim_start_matches(['+','-']);
    if t.is_empty(){return Err("empty decimal".into())}
    let mut dot=0; let mut digit=0; let mut nonzero=false;
    for b in t.bytes(){
        if b==b'.' {dot+=1;if dot>1{return Err("bad decimal".into())}}
        else if b.is_ascii_digit(){digit+=1;if b!=b'0'{nonzero=true}}
        else{return Err("bad decimal".into())}
    }
    if digit==0{return Err("bad decimal".into())}
    Ok(nonzero)
}
fn base_symbol(s:&str)->String{
    let u=norm(s);
    if let Some(x)=u.split('-').next(){if u.contains('-'){return x.to_string()}}
    for q in ["USDT","USDC","BUSD","FDUSD","USD"]{
        if u.ends_with(q)&&u.len()>q.len(){return u[..u.len()-q.len()].to_string()}
    }
    u
}
fn allowed_inventory(path:&Path)->Result<HashSet<String>,String>{
    let text=fs::read_to_string(path).map_err(|e|e.to_string())?;
    let mut found=false; let mut out=HashSet::new();
    for line in text.lines(){
        let trimmed=line.trim();
        if trimmed=="allowed_spot_inventory_bases:"{found=true;continue}
        if found {
            if trimmed.starts_with("- "){out.insert(norm(trimmed.trim_start_matches("- ")));continue}
            if !trimmed.is_empty()&&!trimmed.starts_with('#'){break}
        }
    }
    if !found{return Err("allowed_spot_inventory_bases missing".into())}
    Ok(out)
}
fn req_array<'a>(v:&'a Value,path:&[&str])->Result<&'a Vec<Value>,String>{
    let mut cur=v;
    for k in path{cur=cur.get(*k).ok_or_else(||format!("missing {}",path.join(".")))?}
    cur.as_array().ok_or_else(||format!("{} not array",path.join(".")))
}

fn run(snapshot_path:&Path,state_path:&Path,config_path:&Path)->Result<Value,String>{
    let snapshot:Value=serde_json::from_str(&fs::read_to_string(snapshot_path).map_err(|e|e.to_string())?)
        .map_err(|e|format!("snapshot json:{e}"))?;
    for key in ["r26_private_truth_readonly","read_only_transport","queries_complete"]{
        if snapshot.get(key).and_then(Value::as_bool)!=Some(true){return Err(format!("snapshot {key} not true"))}
    }
    if snapshot.get("network_mutation_possible").and_then(Value::as_bool)!=Some(false){
        return Err("snapshot mutation invariant failed".into())
    }
    let state:Value=serde_json::from_str(&fs::read_to_string(state_path).map_err(|e|e.to_string())?)
        .map_err(|e|format!("state json:{e}"))?;
    let positions=state.get("positions").and_then(Value::as_object).ok_or("state positions missing")?;
    let active:HashSet<String>=positions.keys().map(|x|norm(x)).collect();
    let allowed=allowed_inventory(config_path)?;

    let mut spot=HashSet::new(); let mut futures=HashSet::new(); let mut orders=HashSet::new();
    let mut exchange_assets=HashSet::new();

    for row in req_array(&snapshot,&["detail","okx_balances"])?{
        let ccy=row.get("currency").and_then(Value::as_str).ok_or("okx currency missing")?;
        let bal=row.get("cash_balance").and_then(Value::as_str).ok_or("okx cash_balance missing")?;
        if decimal_nonzero(bal)?&&!quote_asset(ccy){spot.insert(norm(ccy));}
    }
    for row in req_array(&snapshot,&["detail","binance_balances"])?{
        let asset=row.get("asset").and_then(Value::as_str).ok_or("binance asset missing")?;
        let bal=row.get("balance").and_then(Value::as_str).ok_or("binance balance missing")?;
        if decimal_nonzero(bal)?&&!quote_asset(asset){exchange_assets.insert(norm(asset));}
    }
    for row in req_array(&snapshot,&["detail","binance_positions"])?{
        let sym=row.get("symbol").and_then(Value::as_str).ok_or("binance symbol missing")?;
        let amt=row.get("position_amt").and_then(Value::as_str).ok_or("binance position_amt missing")?;
        if decimal_nonzero(amt)?{futures.insert(norm(sym));}
    }
    for row in req_array(&snapshot,&["detail","okx_positions"])?{
        let sym=row.get("inst_id").and_then(Value::as_str).ok_or("okx inst_id missing")?;
        let amt=row.get("position").and_then(Value::as_str).ok_or("okx position missing")?;
        if decimal_nonzero(amt)?{futures.insert(norm(sym));}
    }

    for row in req_array(&snapshot,&["detail","binance_open_orders"])?{
        let sym=row.get("symbol").and_then(Value::as_str).ok_or("binance order symbol missing")?;
        orders.insert(norm(sym));
    }
    for row in req_array(&snapshot,&["detail","okx_pending_spot_orders"])?{
        let sym=row.get("inst_id").and_then(Value::as_str).ok_or("okx order inst_id missing")?;
        orders.insert(norm(sym));
    }

    let mut untracked_spot:Vec<String>=spot.iter()
        .filter(|x|!active.contains(*x)&&!allowed.contains(*x)).cloned().collect();
    let mut untracked_futures:Vec<String>=futures.iter()
        .filter(|x|!active.contains(&base_symbol(x))).cloned().collect();
    let mut untracked_orders:Vec<String>=orders.iter()
        .filter(|x|!active.contains(&base_symbol(x))).cloned().collect();
    let mut untracked_exchange_assets:Vec<String>=exchange_assets.iter()
        .filter(|x|!active.contains(*x)).cloned().collect();
    untracked_spot.sort();untracked_futures.sort();untracked_orders.sort();untracked_exchange_assets.sort();

    let flat_state=active.is_empty();
    let binance_one_way=snapshot.get("summary")
        .and_then(|x|x.get("binance_position_mode"))
        .and_then(Value::as_str)==Some("OneWay");
    let complete=flat_state&&binance_one_way&&untracked_spot.is_empty()&&untracked_futures.is_empty()&&
        untracked_orders.is_empty()&&untracked_exchange_assets.is_empty();

    let mut observed_spot:Vec<_>=spot.into_iter().collect();observed_spot.sort();
    let mut observed_futures:Vec<_>=futures.into_iter().collect();observed_futures.sort();
    let mut observed_orders:Vec<_>=orders.into_iter().collect();observed_orders.sort();
    let mut active_bases:Vec<_>=active.into_iter().collect();active_bases.sort();
    let mut allowed_bases:Vec<_>=allowed.into_iter().collect();allowed_bases.sort();

    Ok(json!({
        "r26_local_reconciliation":complete,
        "verdict":if complete{"PASS"}else{"DENY"},
        "scope":"flat_state_only",
        "flat_state":flat_state,
        "binance_one_way":binance_one_way,
        "deep_active_position_reconciliation_required":!flat_state,
        "state_active_bases":active_bases,
        "allowed_spot_inventory_bases":allowed_bases,
        "observed_nonzero_okx_spot_assets":observed_spot,
        "observed_nonzero_position_symbols":observed_futures,
        "observed_open_order_symbols":observed_orders,
        "untracked_spot":untracked_spot,
        "untracked_futures":untracked_futures,
        "untracked_orders":untracked_orders,
        "untracked_exchange_assets":untracked_exchange_assets
    }))
}

fn main()->Result<(),String>{
    let args:Vec<String>=env::args().collect();
    if args.len()!=4{return Err("usage: r26_local_reconcile SNAPSHOT STATE CONFIG".into())}
    let result=run(Path::new(&args[1]),Path::new(&args[2]),Path::new(&args[3]))?;
    println!("{}",serde_json::to_string_pretty(&result).map_err(|e|e.to_string())?);
    if result.get("r26_local_reconciliation").and_then(Value::as_bool)==Some(true){Ok(())}
    else{Err("R26 reconciliation denied".into())}
}

#[cfg(test)]
mod tests{
    use super::*;
    use std::{fs, process};
    fn files(tag:&str,snapshot:&str,state:&str,config:&str)->(std::path::PathBuf,std::path::PathBuf,std::path::PathBuf){
        let root=env::temp_dir().join(format!("r26rec-{}-{}-{}",process::id(),tag,std::thread::current().name().unwrap_or("t")));
        let _=fs::remove_dir_all(&root);fs::create_dir_all(&root).unwrap();
        let a=root.join("s.json");let b=root.join("state.json");let c=root.join("config.yaml");
        fs::write(&a,snapshot).unwrap();fs::write(&b,state).unwrap();fs::write(&c,config).unwrap();
        (a,b,c)
    }
    fn snap(okx_bal:&str,positions:&str,orders:&str)->String{
        format!(r#"{{"r26_private_truth_readonly":true,"read_only_transport":true,"queries_complete":true,"network_mutation_possible":false,
        "summary":{{"binance_position_mode":"OneWay"}},
        "detail":{{"okx_balances":{okx_bal},"binance_balances":[{{"asset":"USDT","balance":"100","available_balance":"100"}}],
        "binance_positions":{positions},"okx_positions":[],"binance_open_orders":{orders},"okx_pending_spot_orders":[]}}}}"#)
    }
    const CFG:&str="real_trial:\n  allowed_spot_inventory_bases:\n  - BNT\n  - LSK\n";

    #[test]
    fn flat_state_and_allowed_inventory_pass(){
        let s=snap(r#"[{"currency":"USDT","cash_balance":"100"},{"currency":"BNT","cash_balance":"0.9"}]"#,"[]","[]");
        let (a,b,c)=files("pass",&s,r#"{"positions":{}}"#,CFG);
        let r=run(&a,&b,&c).unwrap();
        assert_eq!(r["verdict"],"PASS");
    }

    #[test]
    fn unexpected_spot_inventory_denies(){
        let s=snap(r#"[{"currency":"SOL","cash_balance":"1"}]"#,"[]","[]");
        let (a,b,c)=files("spot",&s,r#"{"positions":{}}"#,CFG);
        let r=run(&a,&b,&c).unwrap();
        assert_eq!(r["verdict"],"DENY");
        assert_eq!(r["untracked_spot"][0],"SOL");
    }

    #[test]
    fn unexpected_futures_position_denies(){
        let s=snap("[]",r#"[{"symbol":"BTCUSDT","position_amt":"-0.01"}]"#,"[]");
        let (a,b,c)=files("fut",&s,r#"{"positions":{}}"#,CFG);
        let r=run(&a,&b,&c).unwrap();
        assert_eq!(r["verdict"],"DENY");
        assert_eq!(r["untracked_futures"][0],"BTCUSDT");
    }

    #[test]
    fn unexpected_open_order_denies(){
        let s=snap("[]","[]",r#"[{"symbol":"ETHUSDT"}]"#);
        let (a,b,c)=files("ord",&s,r#"{"positions":{}}"#,CFG);
        let r=run(&a,&b,&c).unwrap();
        assert_eq!(r["verdict"],"DENY");
        assert_eq!(r["untracked_orders"][0],"ETHUSDT");
    }

    #[test]
    fn active_state_requires_deep_reconciliation(){
        let s=snap("[]","[]","[]");
        let (a,b,c)=files("active",&s,r#"{"positions":{"BTC":{"status":"open"}}}"#,CFG);
        let r=run(&a,&b,&c).unwrap();
        assert_eq!(r["verdict"],"DENY");
        assert_eq!(r["deep_active_position_reconciliation_required"],true);
    }

    #[test]
    fn non_one_way_binance_denies(){
        let mut s:Value=serde_json::from_str(&snap("[]","[]","[]")).unwrap();
        s["summary"]["binance_position_mode"]=Value::String("Hedge".into());
        let (a,b,c)=files("hedge",&serde_json::to_string(&s).unwrap(),r#"{"positions":{}}"#,CFG);
        let r=run(&a,&b,&c).unwrap();
        assert_eq!(r["verdict"],"DENY");
        assert_eq!(r["binance_one_way"],false);
    }

    #[test]
    fn malformed_decimal_fails_closed(){
        assert!(decimal_nonzero("1e-3").is_err());
        assert!(decimal_nonzero("1.2.3").is_err());
    }
}
