//! R26 read-only private account truth normalization.
//! Parses authenticated GET responses only. Any malformed/partial response fails closed.

use crate::private_rest::BinancePositionMode;
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrivateTruthError {
    HttpStatus(u16),
    Json,
    ExchangeCode(String),
    Shape(&'static str),
    Decimal(&'static str),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BinanceAccountConfigTruth {
    pub multi_assets_margin: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BinanceBalanceTruth {
    pub asset: String,
    pub balance: String,
    pub available_balance: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BinancePositionTruth {
    pub symbol: String,
    pub position_amt: String,
    pub position_side: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BinanceOpenOrderTruth {
    pub symbol: String,
    pub order_id: String,
    pub client_order_id: String,
    pub side: String,
    pub position_side: String,
    pub status: String,
    pub reduce_only: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct OkxAccountConfigTruth {
    pub account_level: String,
    pub position_mode: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct OkxBalanceTruth {
    pub currency: String,
    pub cash_balance: String,
    pub available_balance: String,
    pub frozen_balance: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct OkxPositionTruth {
    pub inst_type: String,
    pub inst_id: String,
    pub position: String,
    pub position_side: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct OkxPendingOrderTruth {
    pub inst_id: String,
    pub order_id: String,
    pub client_order_id: String,
    pub state: String,
    pub side: String,
    pub size: String,
    pub filled_size: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PrivateTruthSummary {
    pub binance_position_mode: String,
    pub binance_multi_assets_margin: bool,
    pub binance_nonzero_balances: usize,
    pub binance_nonzero_positions: usize,
    pub binance_open_orders: usize,
    pub okx_account_level: String,
    pub okx_position_mode: String,
    pub okx_nonzero_balances: usize,
    pub okx_nonzero_positions: usize,
    pub okx_pending_spot_orders: usize,
}

fn parse_json(status:u16, body:&str)->Result<Value,PrivateTruthError>{
    if !(200..300).contains(&status){return Err(PrivateTruthError::HttpStatus(status))}
    serde_json::from_str(body).map_err(|_|PrivateTruthError::Json)
}
fn req_str<'a>(v:&'a Value,key:&'static str)->Result<&'a str,PrivateTruthError>{
    v.get(key).and_then(Value::as_str).ok_or(PrivateTruthError::Shape(key))
}
fn req_bool(v:&Value,key:&'static str)->Result<bool,PrivateTruthError>{
    v.get(key).and_then(Value::as_bool).ok_or(PrivateTruthError::Shape(key))
}
fn id_text(v:&Value,key:&'static str)->Result<String,PrivateTruthError>{
    let x=v.get(key).ok_or(PrivateTruthError::Shape(key))?;
    if let Some(s)=x.as_str(){return Ok(s.to_string())}
    if let Some(n)=x.as_i64(){return Ok(n.to_string())}
    if let Some(n)=x.as_u64(){return Ok(n.to_string())}
    Err(PrivateTruthError::Shape(key))
}
fn okx_data(status:u16,body:&str)->Result<Vec<Value>,PrivateTruthError>{
    let v=parse_json(status,body)?;
    let code=req_str(&v,"code")?;
    if code!="0"{return Err(PrivateTruthError::ExchangeCode(code.to_string()))}
    v.get("data").and_then(Value::as_array).cloned().ok_or(PrivateTruthError::Shape("data"))
}
pub fn decimal_is_nonzero(text:&str)->Result<bool,PrivateTruthError>{
    let t=text.trim();
    if t.is_empty(){return Err(PrivateTruthError::Decimal("empty decimal"))}
    let t=t.strip_prefix('+').or_else(||t.strip_prefix('-')).unwrap_or(t);
    if t.is_empty(){return Err(PrivateTruthError::Decimal("sign only"))}
    let mut dots=0usize;let mut digits=0usize;let mut nonzero=false;
    for b in t.bytes(){
        if b==b'.'{dots+=1;if dots>1{return Err(PrivateTruthError::Decimal("multiple dots"))}}
        else if b.is_ascii_digit(){digits+=1;if b!=b'0'{nonzero=true}}
        else{return Err(PrivateTruthError::Decimal("invalid decimal"))}
    }
    if digits==0{return Err(PrivateTruthError::Decimal("no digits"))}
    Ok(nonzero)
}

pub fn parse_binance_account_config(status:u16,body:&str)->Result<BinanceAccountConfigTruth,PrivateTruthError>{
    let v=parse_json(status,body)?;
    Ok(BinanceAccountConfigTruth{multi_assets_margin:req_bool(&v,"multiAssetsMargin")?})
}
pub fn parse_binance_balances(status:u16,body:&str)->Result<Vec<BinanceBalanceTruth>,PrivateTruthError>{
    let v=parse_json(status,body)?;
    let rows=v.as_array().ok_or(PrivateTruthError::Shape("binance balances array"))?;
    rows.iter().map(|x|Ok(BinanceBalanceTruth{
        asset:req_str(x,"asset")?.to_string(),
        balance:req_str(x,"balance")?.to_string(),
        available_balance:req_str(x,"availableBalance")?.to_string(),
    })).collect()
}
pub fn parse_binance_positions(status:u16,body:&str)->Result<Vec<BinancePositionTruth>,PrivateTruthError>{
    let v=parse_json(status,body)?;
    let rows=v.as_array().ok_or(PrivateTruthError::Shape("binance positions array"))?;
    rows.iter().map(|x|Ok(BinancePositionTruth{
        symbol:req_str(x,"symbol")?.to_string(),
        position_amt:req_str(x,"positionAmt")?.to_string(),
        position_side:req_str(x,"positionSide")?.to_string(),
    })).collect()
}
pub fn parse_binance_open_orders(status:u16,body:&str)->Result<Vec<BinanceOpenOrderTruth>,PrivateTruthError>{
    let v=parse_json(status,body)?;
    let rows=v.as_array().ok_or(PrivateTruthError::Shape("binance open orders array"))?;
    rows.iter().map(|x|Ok(BinanceOpenOrderTruth{
        symbol:req_str(x,"symbol")?.to_string(),
        order_id:id_text(x,"orderId")?,
        client_order_id:req_str(x,"clientOrderId")?.to_string(),
        side:req_str(x,"side")?.to_string(),
        position_side:req_str(x,"positionSide")?.to_string(),
        status:req_str(x,"status")?.to_string(),
        reduce_only:req_bool(x,"reduceOnly")?,
    })).collect()
}
pub fn parse_okx_account_config(status:u16,body:&str)->Result<OkxAccountConfigTruth,PrivateTruthError>{
    let rows=okx_data(status,body)?;
    if rows.len()!=1{return Err(PrivateTruthError::Shape("okx config singleton"))}
    Ok(OkxAccountConfigTruth{
        account_level:req_str(&rows[0],"acctLv")?.to_string(),
        position_mode:req_str(&rows[0],"posMode")?.to_string(),
    })
}
pub fn parse_okx_balances(status:u16,body:&str)->Result<Vec<OkxBalanceTruth>,PrivateTruthError>{
    let rows=okx_data(status,body)?;
    if rows.len()!=1{return Err(PrivateTruthError::Shape("okx balance singleton"))}
    let details=rows[0].get("details").and_then(Value::as_array).ok_or(PrivateTruthError::Shape("details"))?;
    details.iter().map(|x|Ok(OkxBalanceTruth{
        currency:req_str(x,"ccy")?.to_string(),
        cash_balance:req_str(x,"cashBal")?.to_string(),
        available_balance:req_str(x,"availBal")?.to_string(),
        frozen_balance:req_str(x,"frozenBal")?.to_string(),
    })).collect()
}
pub fn parse_okx_positions(status:u16,body:&str)->Result<Vec<OkxPositionTruth>,PrivateTruthError>{
    let rows=okx_data(status,body)?;
    rows.iter().map(|x|Ok(OkxPositionTruth{
        inst_type:req_str(x,"instType")?.to_string(),
        inst_id:req_str(x,"instId")?.to_string(),
        position:req_str(x,"pos")?.to_string(),
        position_side:req_str(x,"posSide")?.to_string(),
    })).collect()
}
pub fn parse_okx_pending_orders(status:u16,body:&str)->Result<Vec<OkxPendingOrderTruth>,PrivateTruthError>{
    let rows=okx_data(status,body)?;
    rows.iter().map(|x|Ok(OkxPendingOrderTruth{
        inst_id:req_str(x,"instId")?.to_string(),
        order_id:req_str(x,"ordId")?.to_string(),
        client_order_id:req_str(x,"clOrdId")?.to_string(),
        state:req_str(x,"state")?.to_string(),
        side:req_str(x,"side")?.to_string(),
        size:req_str(x,"sz")?.to_string(),
        filled_size:req_str(x,"accFillSz")?.to_string(),
    })).collect()
}

pub fn summarize(
    binance_position_mode:BinancePositionMode,
    binance_config:&BinanceAccountConfigTruth,
    binance_balances:&[BinanceBalanceTruth],
    binance_positions:&[BinancePositionTruth],
    binance_orders:&[BinanceOpenOrderTruth],
    okx_config:&OkxAccountConfigTruth,
    okx_balances:&[OkxBalanceTruth],
    okx_positions:&[OkxPositionTruth],
    okx_orders:&[OkxPendingOrderTruth],
)->Result<PrivateTruthSummary,PrivateTruthError>{
    let count_nonzero=|values:Vec<&str>|->Result<usize,PrivateTruthError>{
        let mut n=0;for v in values{if decimal_is_nonzero(v)?{n+=1}}Ok(n)
    };
    let bin_bal=count_nonzero(binance_balances.iter().map(|x|x.balance.as_str()).collect())?;
    let bin_pos=count_nonzero(binance_positions.iter().map(|x|x.position_amt.as_str()).collect())?;
    let okx_bal=count_nonzero(okx_balances.iter().map(|x|x.cash_balance.as_str()).collect())?;
    let okx_pos=count_nonzero(okx_positions.iter().map(|x|x.position.as_str()).collect())?;
    Ok(PrivateTruthSummary{
        binance_position_mode:format!("{binance_position_mode:?}"),
        binance_multi_assets_margin:binance_config.multi_assets_margin,
        binance_nonzero_balances:bin_bal,
        binance_nonzero_positions:bin_pos,
        binance_open_orders:binance_orders.len(),
        okx_account_level:okx_config.account_level.clone(),
        okx_position_mode:okx_config.position_mode.clone(),
        okx_nonzero_balances:okx_bal,
        okx_nonzero_positions:okx_pos,
        okx_pending_spot_orders:okx_orders.len(),
    })
}

#[cfg(test)]
mod tests{
    use super::*;
    #[test] fn decimal_classification_is_exact(){
        for z in ["0","0.0","-0.000","+000.000"]{assert!(!decimal_is_nonzero(z).unwrap())}
        for n in ["0.1","-2","000.0001"]{assert!(decimal_is_nonzero(n).unwrap())}
        for bad in ["","-",".","1e-8","1.2.3"]{assert!(decimal_is_nonzero(bad).is_err())}
    }
    #[test] fn binance_fixture_parses_and_summarizes(){
        let c=parse_binance_account_config(200,r#"{"multiAssetsMargin":false}"#).unwrap();
        let b=parse_binance_balances(200,r#"[{"asset":"USDT","balance":"12.5","availableBalance":"10.0"},{"asset":"BNB","balance":"0","availableBalance":"0"}]"#).unwrap();
        let p=parse_binance_positions(200,r#"[{"symbol":"BTCUSDT","positionAmt":"0","positionSide":"BOTH"},{"symbol":"ETHUSDT","positionAmt":"-0.02","positionSide":"BOTH"}]"#).unwrap();
        let o=parse_binance_open_orders(200,r#"[{"symbol":"ETHUSDT","orderId":123,"clientOrderId":"x","side":"BUY","positionSide":"BOTH","status":"NEW","reduceOnly":false}]"#).unwrap();
        let oc=OkxAccountConfigTruth{account_level:"1".into(),position_mode:"net_mode".into()};
        let ob=vec![OkxBalanceTruth{currency:"USDT".into(),cash_balance:"5".into(),available_balance:"5".into(),frozen_balance:"0".into()}];
        let s=summarize(BinancePositionMode::OneWay,&c,&b,&p,&o,&oc,&ob,&[],&[]).unwrap();
        assert_eq!(s.binance_nonzero_balances,1);assert_eq!(s.binance_nonzero_positions,1);assert_eq!(s.binance_open_orders,1);
        assert_eq!(s.okx_nonzero_balances,1);assert_eq!(s.okx_nonzero_positions,0);assert_eq!(s.okx_pending_spot_orders,0);
    }
    #[test] fn okx_fixture_parses(){
        let c=parse_okx_account_config(200,r#"{"code":"0","msg":"","data":[{"acctLv":"1","posMode":"net_mode"}]}"#).unwrap();
        assert_eq!(c.account_level,"1");
        let b=parse_okx_balances(200,r#"{"code":"0","data":[{"details":[{"ccy":"USDT","cashBal":"7.25","availBal":"7","frozenBal":"0.25"}]}]}"#).unwrap();
        assert_eq!(b[0].currency,"USDT");
        let p=parse_okx_positions(200,r#"{"code":"0","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","pos":"0.1","posSide":"net"}]}"#).unwrap();
        assert_eq!(p[0].position,"0.1");
        let o=parse_okx_pending_orders(200,r#"{"code":"0","data":[{"instId":"BTC-USDT","ordId":"44","clOrdId":"abc","state":"live","side":"buy","sz":"1","accFillSz":"0"}]}"#).unwrap();
        assert_eq!(o[0].order_id,"44");
    }
    #[test] fn malformed_or_exchange_error_fails_closed(){
        assert!(matches!(parse_binance_balances(500,"[]"),Err(PrivateTruthError::HttpStatus(500))));
        assert!(matches!(parse_binance_positions(200,"{bad"),Err(PrivateTruthError::Json)));
        assert!(matches!(parse_okx_positions(200,r#"{"code":"50001","data":[]}"#),Err(PrivateTruthError::ExchangeCode(_))));
        assert!(matches!(parse_okx_balances(200,r#"{"code":"0","data":[]}"#),Err(PrivateTruthError::Shape(_))));
    }
}
