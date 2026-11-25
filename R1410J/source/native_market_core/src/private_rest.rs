//! R16 review-only private REST request/signing + response boundary.
//!
//! No sockets are opened here. The module turns an already-authorized R14
//! NetworkMutation into the exact HTTP request bytes/headers expected by
//! OKX/Binance and parses deterministic response bodies. This keeps signing
//! independently certifiable before any live transport is connected.

use crate::authoritative_state::{AuthorizedEntryOrder,NativeEntryLeg};
use crate::execution::{Action as ExecAction, Leg};
use crate::execution_adapter::{
    authorize_exact, AdapterError, OrderIntent, OrderKind, ResponseClass, Venue,
};
use crate::fill_economics::{
    decimal_to_atoms as econ_decimal_to_atoms, signed_decimal_to_atoms as econ_signed_decimal_to_atoms,
    EconError, FillEvent, LegFillLedger, DEFAULT_AVG_TOLERANCE_ATOMS,
};
#[cfg(test)]
use crate::execution_store::DurableMutationGate;
use crate::position_lifecycle::{
    MutationObservation as LifecycleMutationObservation, RecoveryEvidence,
};
use openssl::{base64::encode_block, hash::MessageDigest, pkey::PKey, sign::Signer};
use serde::Serialize;
use serde_json::Value;
use std::fmt;
use url::form_urlencoded;

pub const BINANCE_DEFAULT_RECV_WINDOW_MS: u64 = 10_000;
pub const ECON_CUMQUOTE_TOLERANCE_ATOMS: u128 = 100_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OkxDomain {
    Global,
    Eea,
    Us,
    OpenApi,
}

impl OkxDomain {
    pub fn base_url(self) -> &'static str {
        match self {
            Self::Global => "https://www.okx.com",
            Self::Eea => "https://eea.okx.com",
            Self::Us => "https://us.okx.com",
            Self::OpenApi => "https://openapi.okx.com",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinancePositionMode {
    OneWay,
    Hedge,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestBuildError {
    MissingCredential,
    CredentialVenueMismatch,
    InvalidBoundaryAction,
    InvalidSymbol,
    MissingPrice,
    UnsupportedOrderKind,
    UnsupportedBinancePositionMode,
    InvalidRecvWindow,
    InvalidQuantity,
    InsufficientRecoveryEvidence,
    NativeEntryPermitRequired,
    StaleSafety,
    Adapter(AdapterError),
    Crypto,
    Json,
}

impl From<AdapterError> for RestBuildError {
    fn from(value: AdapterError) -> Self {
        if value == AdapterError::StaleSafety {
            Self::StaleSafety
        } else {
            Self::Adapter(value)
        }
    }
}

pub struct OkxCredentials {
    api_key: String,
    secret: String,
    passphrase: String,
}

impl OkxCredentials {
    pub fn new(api_key: &str, secret: &str, passphrase: &str) -> Result<Self, RestBuildError> {
        if api_key.is_empty() || secret.is_empty() || passphrase.is_empty() {
            return Err(RestBuildError::MissingCredential);
        }
        Ok(Self {
            api_key: api_key.into(),
            secret: secret.into(),
            passphrase: passphrase.into(),
        })
    }
}

pub struct BinanceCredentials {
    api_key: String,
    secret: String,
}

impl BinanceCredentials {
    pub fn new(api_key: &str, secret: &str) -> Result<Self, RestBuildError> {
        if api_key.is_empty() || secret.is_empty() {
            return Err(RestBuildError::MissingCredential);
        }
        Ok(Self {
            api_key: api_key.into(),
            secret: secret.into(),
        })
    }
}

pub enum CredentialsRef<'a> {
    Okx(&'a OkxCredentials),
    Binance(&'a BinanceCredentials),
}

#[derive(Clone, PartialEq, Eq)]
pub struct SignedRequest {
    pub venue: Venue,
    pub method: HttpMethod,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
    pub client_order_id: Option<String>,
    pub is_mutation: bool,
}

impl fmt::Debug for SignedRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header_names: Vec<&str> = self.headers.iter().map(|x| x.0.as_str()).collect();
        f.debug_struct("SignedRequest")
            .field("venue", &self.venue)
            .field("method", &self.method)
            .field("url", &"<redacted-signed-url>")
            .field("headers", &header_names)
            .field("body", &self.body.as_deref().map(|_| "<redacted-signed-body>"))
            .field("client_order_id", &self.client_order_id)
            .field("is_mutation", &self.is_mutation)
            .finish()
    }
}

pub struct PreparedEntryRequest{
    pub(crate) authorized:AuthorizedEntryOrder,
    pub(crate) request:SignedRequest,
}
impl fmt::Debug for PreparedEntryRequest{
    fn fmt(&self,f:&mut fmt::Formatter<'_>)->fmt::Result{
        f.debug_struct("PreparedEntryRequest")
            .field("leg",&self.authorized.leg())
            .field("generation",&self.authorized.generation())
            .field("intent_id",&self.authorized.intent_id())
            .field("request",&self.request)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestOutcomeClass {
    Acknowledged,
    Rejected,
    Unknown,
}

impl RestOutcomeClass {
    pub fn adapter_class(self) -> ResponseClass {
        match self {
            Self::Acknowledged => ResponseClass::Acknowledged,
            Self::Rejected => ResponseClass::Rejected,
            Self::Unknown => ResponseClass::Unknown,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderObservation {
    pub class: RestOutcomeClass,
    pub terminal: bool,
    pub filled: Option<String>,
    pub status: Option<String>,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub reason: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EconomicTruthClass { Known, Rejected, Unknown }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderEconomicTruth {
    pub class: EconomicTruthClass,
    pub terminal: bool,
    pub cumulative_qty_atoms: Option<u128>,
    pub avg_price_atoms: Option<u128>,
    pub cumulative_quote_atoms: Option<u128>,
    pub status: Option<String>,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub observed_fill_time_ms: Option<u64>,
    pub reason: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EconomicTruthError {
    InvalidResponse,
    IdentityMismatch,
    Economic(EconError),
}
impl From<EconError> for EconomicTruthError { fn from(v:EconError)->Self{Self::Economic(v)} }

#[cfg(test)]
fn header<'a>(request: &'a SignedRequest, name: &str) -> Option<&'a str> {
    request
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

fn hmac_sha256(secret: &str, payload: &[u8]) -> Result<Vec<u8>, RestBuildError> {
    let key = PKey::hmac(secret.as_bytes()).map_err(|_| RestBuildError::Crypto)?;
    let mut signer =
        Signer::new(MessageDigest::sha256(), &key).map_err(|_| RestBuildError::Crypto)?;
    signer
        .update(payload)
        .map_err(|_| RestBuildError::Crypto)?;
    signer.sign_to_vec().map_err(|_| RestBuildError::Crypto)
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

pub fn okx_timestamp_from_unix_ms(unix_ms: u64) -> String {
    let total_sec = unix_ms / 1000;
    let millis = unix_ms % 1000;
    let days = (total_sec / 86_400) as i64;
    let sec_of_day = total_sec % 86_400;
    let hour = sec_of_day / 3600;
    let minute = (sec_of_day % 3600) / 60;
    let second = sec_of_day % 60;

    // Howard Hinnant civil_from_days, with Unix epoch offset.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z"
    )
}

fn action_leg(action: &ExecAction) -> Result<Leg, RestBuildError> {
    match action {
        ExecAction::NetworkMutation { leg, .. } => Ok(*leg),
        _ => Err(RestBuildError::InvalidBoundaryAction),
    }
}

fn validate_boundary(
    action: &ExecAction,
    intent: &OrderIntent,
    safety_generation: u64,
    current_generation: u64,
) -> Result<(), RestBuildError> {
    let leg = action_leg(action)?;
    let expected_leg = match intent.venue {
        Venue::OkxSpot => Leg::Spot,
        Venue::BinanceUsdm => Leg::Futures,
    };
    if leg != expected_leg {
        return Err(RestBuildError::InvalidBoundaryAction);
    }
    authorize_exact(
        intent,
        true,
        &intent.client_id,
        safety_generation,
        current_generation,
    )?;
    Ok(())
}

fn okx_symbol(value: &str) -> Result<String, RestBuildError> {
    let raw = value.trim().to_ascii_uppercase();
    if raw.is_empty() || raw.contains(':') {
        return Err(RestBuildError::InvalidSymbol);
    }
    let out = raw.replace('/', "-");
    let mut parts = out.split('-');
    if parts.next().unwrap_or("").is_empty()
        || parts.next().unwrap_or("").is_empty()
        || parts.next().is_some()
    {
        return Err(RestBuildError::InvalidSymbol);
    }
    Ok(out)
}

fn binance_symbol(value: &str) -> Result<String, RestBuildError> {
    let raw = value.trim().to_ascii_uppercase();
    if raw.is_empty() {
        return Err(RestBuildError::InvalidSymbol);
    }
    let main = raw.split(':').next().unwrap_or("");
    let out = main.replace('/', "").replace('-', "");
    if out.is_empty() || !out.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return Err(RestBuildError::InvalidSymbol);
    }
    Ok(out)
}

#[derive(Serialize)]
struct OkxOrderBody<'a> {
    #[serde(rename = "instId")]
    inst_id: &'a str,
    #[serde(rename = "tdMode")]
    td_mode: &'static str,
    #[serde(rename = "clOrdId")]
    client_order_id: &'a str,
    side: &'a str,
    #[serde(rename = "ordType")]
    order_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    px: Option<&'a str>,
    sz: &'a str,
    #[serde(rename = "tgtCcy")]
    target_ccy: &'static str,
}

fn build_okx_order(
    intent: &OrderIntent,
    kind: OrderKind,
    price: Option<&str>,
    creds: &OkxCredentials,
    unix_ms: u64,
    domain: OkxDomain,
) -> Result<SignedRequest, RestBuildError> {
    if intent.venue != Venue::OkxSpot {
        return Err(RestBuildError::CredentialVenueMismatch);
    }
    let symbol = okx_symbol(&intent.symbol)?;
    let (order_type, px) = match kind {
        OrderKind::Market => ("market", None),
        OrderKind::AggressiveLimitFok => {
            let px = price.filter(|x| !x.is_empty()).ok_or(RestBuildError::MissingPrice)?;
            ("fok", Some(px))
        }
    };
    let body = serde_json::to_string(&OkxOrderBody {
        inst_id: &symbol,
        td_mode: "cash",
        client_order_id: &intent.client_id,
        side: &intent.side,
        order_type,
        px,
        sz: &intent.amount,
        target_ccy: "base_ccy",
    })
    .map_err(|_| RestBuildError::Json)?;
    let path = "/api/v5/trade/order";
    let timestamp = okx_timestamp_from_unix_ms(unix_ms);
    let prehash = format!("{timestamp}POST{path}{body}");
    let sig = encode_block(&hmac_sha256(&creds.secret, prehash.as_bytes())?);
    Ok(SignedRequest {
        venue: Venue::OkxSpot,
        method: HttpMethod::Post,
        url: format!("{}{path}", domain.base_url()),
        headers: vec![
            ("Content-Type".into(), "application/json".into()),
            ("OK-ACCESS-KEY".into(), creds.api_key.clone()),
            ("OK-ACCESS-PASSPHRASE".into(), creds.passphrase.clone()),
            ("OK-ACCESS-TIMESTAMP".into(), timestamp),
            ("OK-ACCESS-SIGN".into(), sig),
        ],
        body: Some(body),
        client_order_id: Some(intent.client_id.clone()),
        is_mutation: true,
    })
}

fn form_body(pairs: &[(&str, String)]) -> String {
    let mut ser = form_urlencoded::Serializer::new(String::new());
    for (k, v) in pairs {
        ser.append_pair(k, v);
    }
    ser.finish()
}

fn build_binance_order(
    intent: &OrderIntent,
    reduce_only: bool,
    creds: &BinanceCredentials,
    unix_ms: u64,
    recv_window_ms: u64,
    mode: BinancePositionMode,
) -> Result<SignedRequest, RestBuildError> {
    if intent.venue != Venue::BinanceUsdm {
        return Err(RestBuildError::CredentialVenueMismatch);
    }
    if mode != BinancePositionMode::OneWay {
        return Err(RestBuildError::UnsupportedBinancePositionMode);
    }
    if recv_window_ms == 0 || recv_window_ms > 60_000 {
        return Err(RestBuildError::InvalidRecvWindow);
    }
    let symbol = binance_symbol(&intent.symbol)?;
    let mut pairs = vec![
        ("timestamp", unix_ms.to_string()),
        ("symbol", symbol),
        ("side", intent.side.to_ascii_uppercase()),
        ("type", "MARKET".into()),
        ("quantity", intent.amount.clone()),
        ("newClientOrderId", intent.client_id.clone()),
        ("newOrderRespType", "RESULT".into()),
    ];
    if reduce_only {
        pairs.push(("reduceOnly", "true".into()));
    }
    pairs.push(("recvWindow", recv_window_ms.to_string()));
    let unsigned = form_body(&pairs);
    let signature = hex_lower(&hmac_sha256(&creds.secret, unsigned.as_bytes())?);
    let body = format!("{unsigned}&signature={signature}");
    Ok(SignedRequest {
        venue: Venue::BinanceUsdm,
        method: HttpMethod::Post,
        url: "https://fapi.binance.com/fapi/v1/order".into(),
        headers: vec![
            ("Content-Type".into(), "application/x-www-form-urlencoded".into()),
            ("X-MBX-APIKEY".into(), creds.api_key.clone()),
        ],
        body: Some(body),
        client_order_id: Some(intent.client_id.clone()),
        is_mutation: true,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn build_signed_order_from_network_boundary(
    action: &ExecAction,
    intent: &OrderIntent,
    kind: OrderKind,
    reduce_only: bool,
    price: Option<&str>,
    safety_generation: u64,
    current_generation: u64,
    credentials: CredentialsRef<'_>,
    unix_ms: u64,
    okx_domain: OkxDomain,
    binance_position_mode: BinancePositionMode,
    binance_recv_window_ms: u64,
) -> Result<SignedRequest, RestBuildError> {
    validate_boundary(action, intent, safety_generation, current_generation)?;
    let native_entry_shape=
        (intent.venue==Venue::OkxSpot&&intent.side.eq_ignore_ascii_case("buy")&&!reduce_only)||
        (intent.venue==Venue::BinanceUsdm&&intent.side.eq_ignore_ascii_case("sell")&&!reduce_only);
    if native_entry_shape{return Err(RestBuildError::NativeEntryPermitRequired)}
    match (intent.venue, credentials) {
        (Venue::OkxSpot, CredentialsRef::Okx(creds)) => {
            if reduce_only {
                return Err(RestBuildError::UnsupportedOrderKind);
            }
            build_okx_order(intent, kind, price, creds, unix_ms, okx_domain)
        }
        (Venue::BinanceUsdm, CredentialsRef::Binance(creds)) => {
            if kind != OrderKind::Market {
                return Err(RestBuildError::UnsupportedOrderKind);
            }
            build_binance_order(
                intent,
                reduce_only,
                creds,
                unix_ms,
                binance_recv_window_ms,
                binance_position_mode,
            )
        }
        _ => Err(RestBuildError::CredentialVenueMismatch),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_signed_entry_order_from_native_authority(
    authorized:AuthorizedEntryOrder,
    kind:OrderKind,
    price:Option<&str>,
    credentials:CredentialsRef<'_>,
    unix_ms:u64,
    okx_domain:OkxDomain,
    binance_position_mode:BinancePositionMode,
    binance_recv_window_ms:u64,
)->Result<PreparedEntryRequest,RestBuildError>{
    let intent=authorized.intent();
    let request=match (authorized.leg(),intent.venue,credentials){
        (NativeEntryLeg::Spot,Venue::OkxSpot,CredentialsRef::Okx(creds))=>{
            if !intent.side.eq_ignore_ascii_case("buy")||kind!=OrderKind::AggressiveLimitFok{
                return Err(RestBuildError::UnsupportedOrderKind)
            }
            build_okx_order(intent,kind,price,creds,unix_ms,okx_domain)?
        }
        (NativeEntryLeg::Futures,Venue::BinanceUsdm,CredentialsRef::Binance(creds))=>{
            if !intent.side.eq_ignore_ascii_case("sell")||kind!=OrderKind::Market||price.is_some(){
                return Err(RestBuildError::UnsupportedOrderKind)
            }
            build_binance_order(
                intent,false,creds,unix_ms,binance_recv_window_ms,binance_position_mode
            )?
        }
        _=>return Err(RestBuildError::CredentialVenueMismatch),
    };
    Ok(PreparedEntryRequest{authorized,request})
}

pub fn build_okx_query_by_client_id(
    creds: &OkxCredentials,
    unix_ms: u64,
    domain: OkxDomain,
    symbol: &str,
    client_order_id: &str,
) -> Result<SignedRequest, RestBuildError> {
    let symbol = okx_symbol(symbol)?;
    let mut ser = form_urlencoded::Serializer::new(String::new());
    ser.append_pair("instId", &symbol);
    ser.append_pair("clOrdId", client_order_id);
    let query = ser.finish();
    let path = format!("/api/v5/trade/order?{query}");
    let timestamp = okx_timestamp_from_unix_ms(unix_ms);
    let prehash = format!("{timestamp}GET{path}");
    let sig = encode_block(&hmac_sha256(&creds.secret, prehash.as_bytes())?);
    Ok(SignedRequest {
        venue: Venue::OkxSpot,
        method: HttpMethod::Get,
        url: format!("{}{}", domain.base_url(), path),
        headers: vec![
            ("OK-ACCESS-KEY".into(), creds.api_key.clone()),
            ("OK-ACCESS-PASSPHRASE".into(), creds.passphrase.clone()),
            ("OK-ACCESS-TIMESTAMP".into(), timestamp),
            ("OK-ACCESS-SIGN".into(), sig),
        ],
        body: None,
        client_order_id: Some(client_order_id.into()),
        is_mutation: false,
    })
}

pub fn build_binance_query_by_client_id(
    creds: &BinanceCredentials,
    unix_ms: u64,
    recv_window_ms: u64,
    symbol: &str,
    client_order_id: &str,
) -> Result<SignedRequest, RestBuildError> {
    let symbol = binance_symbol(symbol)?;
    let pairs = vec![
        ("timestamp", unix_ms.to_string()),
        ("symbol", symbol),
        ("origClientOrderId", client_order_id.to_string()),
        ("recvWindow", recv_window_ms.to_string()),
    ];
    let unsigned = form_body(&pairs);
    let signature = hex_lower(&hmac_sha256(&creds.secret, unsigned.as_bytes())?);
    let query = format!("{unsigned}&signature={signature}");
    Ok(SignedRequest {
        venue: Venue::BinanceUsdm,
        method: HttpMethod::Get,
        url: format!("https://fapi.binance.com/fapi/v1/order?{query}"),
        headers: vec![("X-MBX-APIKEY".into(), creds.api_key.clone())],
        body: None,
        client_order_id: Some(client_order_id.into()),
        is_mutation: false,
    })
}

pub fn build_okx_fills_history_request(
    creds: &OkxCredentials,
    unix_ms: u64,
    domain: OkxDomain,
    symbol: &str,
    order_id: &str,
) -> Result<SignedRequest, RestBuildError> {
    let symbol = okx_symbol(symbol)?;
    if order_id.trim().is_empty() { return Err(RestBuildError::InvalidSymbol); }
    let mut ser = form_urlencoded::Serializer::new(String::new());
    ser.append_pair("instType", "SPOT");
    ser.append_pair("instId", &symbol);
    ser.append_pair("ordId", order_id);
    let query = ser.finish();
    let path = format!("/api/v5/trade/fills-history?{query}");
    let timestamp = okx_timestamp_from_unix_ms(unix_ms);
    let prehash = format!("{timestamp}GET{path}");
    let sig = encode_block(&hmac_sha256(&creds.secret, prehash.as_bytes())?);
    Ok(SignedRequest { venue: Venue::OkxSpot, method: HttpMethod::Get,
        url: format!("{}{}",domain.base_url(),path),
        headers: vec![("OK-ACCESS-KEY".into(),creds.api_key.clone()),
            ("OK-ACCESS-PASSPHRASE".into(),creds.passphrase.clone()),
            ("OK-ACCESS-TIMESTAMP".into(),timestamp),("OK-ACCESS-SIGN".into(),sig)],
        body:None,client_order_id:None,is_mutation:false })
}

pub fn build_binance_user_trades_request(
    creds:&BinanceCredentials,
    unix_ms:u64,
    recv_window_ms:u64,
    symbol:&str,
    order_id:&str,
)->Result<SignedRequest,RestBuildError>{
    if recv_window_ms==0||recv_window_ms>60_000{return Err(RestBuildError::InvalidRecvWindow)}
    let symbol=binance_symbol(symbol)?;
    if order_id.trim().is_empty(){return Err(RestBuildError::InvalidSymbol)}
    let pairs=vec![("timestamp",unix_ms.to_string()),("symbol",symbol),("orderId",order_id.to_string()),("recvWindow",recv_window_ms.to_string())];
    let unsigned=form_body(&pairs);let signature=hex_lower(&hmac_sha256(&creds.secret,unsigned.as_bytes())?);
    let query=format!("{unsigned}&signature={signature}");
    Ok(SignedRequest{venue:Venue::BinanceUsdm,method:HttpMethod::Get,
        url:format!("https://fapi.binance.com/fapi/v1/userTrades?{query}"),
        headers:vec![("X-MBX-APIKEY".into(),creds.api_key.clone())],body:None,client_order_id:None,is_mutation:false})
}


/// R32D4: signed, strictly read-only paged SPOT fills by OKX billId.
/// Pagination uses 'after' for records older than the supplied billId.
/// The limit is bounded to the published OKX maximum of 100.
pub fn build_okx_fills_history_page_request(
    creds:&OkxCredentials,unix_ms:u64,domain:OkxDomain,
    symbol:&str,order_id:&str,after_bill_id:Option<&str>,limit:u16,
)->Result<SignedRequest,RestBuildError>{
    if limit==0||limit>100||order_id.is_empty()
        ||!order_id.bytes().all(|c|c.is_ascii_digit()){
        return Err(RestBuildError::InvalidSymbol)
    }
    let symbol=okx_symbol(symbol)?;
    let mut ser=form_urlencoded::Serializer::new(String::new());
    ser.append_pair("instType","SPOT");
    ser.append_pair("instId",&symbol);
    ser.append_pair("ordId",order_id);
    ser.append_pair("limit",&limit.to_string());
    if let Some(bill)=after_bill_id{
        if bill.is_empty()||!bill.bytes().all(|c|c.is_ascii_digit()){
            return Err(RestBuildError::InvalidSymbol)
        }
        ser.append_pair("after",bill);
    }
    build_okx_signed_get(creds,unix_ms,domain,
        &format!("/api/v5/trade/fills-history?{}",ser.finish()))
}

/// R32D4: signed, strictly read-only Binance USDT-M fills pagination.
/// Uses ascending trade ID fromId (inclusive), with exact orderId filter.
pub fn build_binance_user_trades_page_request(
    creds:&BinanceCredentials,unix_ms:u64,recv_window_ms:u64,
    symbol:&str,order_id:&str,from_id:u64,limit:u16,
)->Result<SignedRequest,RestBuildError>{
    if limit==0||limit>1000||order_id.is_empty()
        ||!order_id.bytes().all(|c|c.is_ascii_digit()){
        return Err(RestBuildError::InvalidSymbol)
    }
    let symbol=binance_symbol(symbol)?;
    build_binance_signed_get(creds,unix_ms,recv_window_ms,
        "/fapi/v1/userTrades",
        &[("symbol",symbol),("orderId",order_id.into()),
          ("fromId",from_id.to_string()),("limit",limit.to_string())])
}

fn build_binance_signed_get(
    creds:&BinanceCredentials,
    unix_ms:u64,
    recv_window_ms:u64,
    path:&str,
    extra:&[(&str,String)],
)->Result<SignedRequest,RestBuildError>{
    if recv_window_ms==0||recv_window_ms>60_000{return Err(RestBuildError::InvalidRecvWindow)}
    let mut pairs=extra.to_vec();
    pairs.push(("timestamp",unix_ms.to_string()));
    pairs.push(("recvWindow",recv_window_ms.to_string()));
    let unsigned=form_body(&pairs);
    let signature=hex_lower(&hmac_sha256(&creds.secret,unsigned.as_bytes())?);
    let query=format!("{unsigned}&signature={signature}");
    Ok(SignedRequest{
        venue:Venue::BinanceUsdm,method:HttpMethod::Get,
        url:format!("https://fapi.binance.com{path}?{query}"),
        headers:vec![("X-MBX-APIKEY".into(),creds.api_key.clone())],
        body:None,client_order_id:None,is_mutation:false,
    })
}

fn build_okx_signed_get(
    creds:&OkxCredentials,
    unix_ms:u64,
    domain:OkxDomain,
    path_with_query:&str,
)->Result<SignedRequest,RestBuildError>{
    let timestamp=okx_timestamp_from_unix_ms(unix_ms);
    let prehash=format!("{timestamp}GET{path_with_query}");
    let sig=encode_block(&hmac_sha256(&creds.secret,prehash.as_bytes())?);
    Ok(SignedRequest{
        venue:Venue::OkxSpot,method:HttpMethod::Get,
        url:format!("{}{}",domain.base_url(),path_with_query),
        headers:vec![
            ("OK-ACCESS-KEY".into(),creds.api_key.clone()),
            ("OK-ACCESS-PASSPHRASE".into(),creds.passphrase.clone()),
            ("OK-ACCESS-TIMESTAMP".into(),timestamp),
            ("OK-ACCESS-SIGN".into(),sig),
        ],
        body:None,client_order_id:None,is_mutation:false,
    })
}

/// R32D5B: USD-M income history, deliberately WITHOUT symbol or incomeType
/// filters. Transfers, funding fees, rebates, and other unexpected wallet
/// movements must stay visible to conservative recovery checks.
pub fn build_binance_all_income_page_request(
    creds:&BinanceCredentials,unix_ms:u64,recv_window_ms:u64,
    begin_ms:u64,end_ms:u64,page:u16,limit:u16,
)->Result<SignedRequest,RestBuildError>{
    if begin_ms==0||end_ms<begin_ms||end_ms>unix_ms
        ||page==0||page>16||limit==0||limit>1000{
        return Err(RestBuildError::InvalidQuantity)
    }
    build_binance_signed_get(creds,unix_ms,recv_window_ms,
        "/fapi/v1/income",
        &[("startTime",begin_ms.to_string()),("endTime",end_ms.to_string()),
          ("page",page.to_string()),("limit",limit.to_string())])
}

/// P0-03: no-page USD-M /income time-window alternative. Binance's current
/// docs list no page, but its official SDK also accepts page: do NOT infer
/// older paged Rust readers are wrong without real API compatibility proof.
/// A full time-window page remains INCOMPLETE; same-ms ties need auditing.
pub fn build_binance_income_time_window_request(
    creds:&BinanceCredentials,now:u64,recv:u64,
    start:u64,end:u64,limit:u16,
)->Result<SignedRequest,RestBuildError>{
    if start==0||end<start||end>now||limit==0||limit>1000
       ||end-start>7*24*60*60*1000{
      return Err(RestBuildError::InvalidQuantity)
    }
    build_binance_signed_get(creds,now,recv,"/fapi/v1/income",
      &[("startTime",start.to_string()),("endTime",end.to_string()),
        ("limit",limit.to_string())])
}

/// R32D6A: signed GET all executions for one symbol, without orderId.
/// Mandatory <=7-day time window, strict bounded response count.
pub fn build_binance_symbol_trade_window_request(
    creds:&BinanceCredentials,unix_ms:u64,recv_window_ms:u64,
    symbol:&str,begin_ms:u64,end_ms:u64,limit:u16,
)->Result<SignedRequest,RestBuildError>{
    if begin_ms==0 || end_ms<begin_ms || end_ms>unix_ms
        ||end_ms-begin_ms>7*24*60*60*1000
        ||limit==0 || limit>1000 {
        return Err(RestBuildError::InvalidQuantity)
    }
    let name=binance_symbol(symbol)?;
    build_binance_signed_get(creds,unix_ms,recv_window_ms,"/fapi/v1/userTrades",
        &[("symbol",name),("startTime",begin_ms.to_string()),
          ("endTime",end_ms.to_string()),("limit",limit.to_string())])
}
pub fn build_binance_account_config_request(
    creds:&BinanceCredentials,unix_ms:u64,recv_window_ms:u64
)->Result<SignedRequest,RestBuildError>{
    build_binance_signed_get(creds,unix_ms,recv_window_ms,"/fapi/v1/accountConfig",&[])
}
pub fn build_binance_balance_v3_request(
    creds:&BinanceCredentials,unix_ms:u64,recv_window_ms:u64
)->Result<SignedRequest,RestBuildError>{
    build_binance_signed_get(creds,unix_ms,recv_window_ms,"/fapi/v3/balance",&[])
}
pub fn build_binance_position_risk_v3_request(
    creds:&BinanceCredentials,unix_ms:u64,recv_window_ms:u64
)->Result<SignedRequest,RestBuildError>{
    build_binance_signed_get(creds,unix_ms,recv_window_ms,"/fapi/v3/positionRisk",&[])
}
/// TRADE2D2D: ALL-SYMBOL USD-M open algo orders, no symbol OR algoType
/// filter; documented 40 IP weight on Binance. GET-only.
/// TRADE2D2E1: bounded unfiltered Binance USD-M algo order history.
/// No symbol/algo type filter. Maximum one 7-day API query; a result
/// beyond the documented retention cannot be considered complete.
/// TRADE2D2E2: resolve ONE Binance conditional parent algo ID.
pub fn build_binance_algo_by_id_request(
    creds:&BinanceCredentials,now:u64,recv:u64,algo_id:u64
)->Result<SignedRequest,RestBuildError>{
    if algo_id==0{return Err(RestBuildError::InvalidQuantity)}
    build_binance_signed_get(creds,now,recv,
        "/fapi/v1/algoOrder",&[("algoId",algo_id.to_string())])
}
/// TRADE2D2E2: read original ordinary child order ID, no mutable action.
pub fn build_binance_child_by_order_id_request(
    creds:&BinanceCredentials,now:u64,recv:u64,
    symbol:&str,order_id:u64,
)->Result<SignedRequest,RestBuildError>{
    if order_id==0{return Err(RestBuildError::InvalidQuantity)}
    build_binance_signed_get(creds,now,recv,"/fapi/v1/order",
        &[("symbol",binance_symbol(symbol)?),("orderId",order_id.to_string())])
}
/// TRADE2D2E2: resolve ONE OKX algo parent from documented algoId.
pub fn build_okx_algo_by_id_request(
    creds:&OkxCredentials,now:u64,domain:OkxDomain,algo_id:&str,
)->Result<SignedRequest,RestBuildError>{
    if algo_id.is_empty()||!algo_id.bytes().all(|x|x.is_ascii_digit()){
       return Err(RestBuildError::InvalidSymbol)
    }
    build_okx_signed_get(creds,now,domain,
        &format!("/api/v5/trade/order-algo?algoId={algo_id}"))
}
/// TRADE2D2E2: resolve ordinary child by exchange ID.
pub fn build_okx_child_by_order_id_request(
    creds:&OkxCredentials,now:u64,domain:OkxDomain,
    symbol:&str,order_id:&str,
)->Result<SignedRequest,RestBuildError>{
    if order_id.is_empty()||!order_id.bytes().all(|x|x.is_ascii_digit()){
       return Err(RestBuildError::InvalidSymbol)
    }
    build_okx_signed_get(creds,now,domain,
        &format!("/api/v5/trade/order?instId={}&ordId={order_id}",
                 okx_symbol(symbol)?))
}

pub fn build_binance_all_algo_orders_history_request(
    creds:&BinanceCredentials,now:u64,recv:u64,start:u64,end:u64,
)->Result<SignedRequest,RestBuildError>{
    if start==0||end<start||end>now||end-start>=7*24*60*60*1000{
        return Err(RestBuildError::InvalidQuantity)
    }
    build_binance_signed_get(creds,now,recv,
        "/fapi/v1/allAlgoOrders",
        &[("startTime",start.to_string()),("endTime",end.to_string()),
          ("limit","1000".into())])
}
/// TRADE2D2E1: OKX all-symbol history of one documented algo family
/// and one of the documented terminal states. No instrument filtering.
pub fn build_okx_algo_history_type_state_request(
    creds:&OkxCredentials,now:u64,domain:OkxDomain,
    kind:&str,state:&str,
)->Result<SignedRequest,RestBuildError>{
    match kind{
        "conditional"|"oco"|"trigger"|"move_order_stop"|"chase"|
        "iceberg"|"twap"|"smart_iceberg"=>{},
        _=>return Err(RestBuildError::InvalidQuantity)
    }
    if !matches!(state,"effective"|"canceled"|"order_failed"){
        return Err(RestBuildError::InvalidQuantity)
    }
    build_okx_signed_get(creds,now,domain,
        &format!("/api/v5/trade/orders-algo-history?ordType={kind}&state={state}&limit=100"))
}

pub fn build_binance_all_open_algo_orders_request(
    creds:&BinanceCredentials,unix_ms:u64,recv_window_ms:u64,
)->Result<SignedRequest,RestBuildError>{
    build_binance_signed_get(creds,unix_ms,recv_window_ms,
        "/fapi/v1/openAlgoOrders",&[])
}

/// TRADE2D2D: OKX ALL-SYMBOL pending algo orders for one documented ordType.
/// Do not attach instType/instId: recovery must not miss foreign symbols.
/// Query only recognised documented families; never accept caller URL.
pub fn build_okx_pending_algo_type_request(
    creds:&OkxCredentials,unix_ms:u64,domain:OkxDomain,kind:&str,
)->Result<SignedRequest,RestBuildError>{
    match kind{
        "conditional"|"oco"|"trigger"|"move_order_stop"|"chase"|
        "iceberg"|"twap"|"smart_iceberg"=>{},
        _=>return Err(RestBuildError::InvalidQuantity)
    }
    build_okx_signed_get(creds,unix_ms,domain,
        &format!("/api/v5/trade/orders-algo-pending?ordType={kind}&limit=100"))
}

pub fn build_binance_open_orders_request(
    creds:&BinanceCredentials,unix_ms:u64,recv_window_ms:u64
)->Result<SignedRequest,RestBuildError>{
    build_binance_signed_get(creds,unix_ms,recv_window_ms,"/fapi/v1/openOrders",&[])
}

/// R32D5: account bill archive, ALL base-currency balance movements.
/// Do NOT filter instType=SPOT: transfers and rewards are relevant too.
/// Cursor 'after' means earlier bill IDs, max 100 rows per page.
pub fn build_okx_base_currency_bills_page_request(
    creds:&OkxCredentials,unix_ms:u64,domain:OkxDomain,
    base:&str,begin_ms:u64,end_ms:u64,
    after_bill_id:Option<&str>,limit:u16,
)->Result<SignedRequest,RestBuildError>{
    if base.is_empty()||!base.bytes().all(|c|c.is_ascii_uppercase())
        ||begin_ms==0||end_ms<begin_ms||limit==0||limit>100{
        return Err(RestBuildError::InvalidSymbol)
    }
    let mut ser=form_urlencoded::Serializer::new(String::new());
    ser.append_pair("ccy",base);
    ser.append_pair("begin",&begin_ms.to_string());
    ser.append_pair("end",&end_ms.to_string());
    ser.append_pair("limit",&limit.to_string());
    if let Some(bill)=after_bill_id{
        if bill.is_empty()||!bill.bytes().all(|b|b.is_ascii_digit()){
            return Err(RestBuildError::InvalidSymbol)
        }
        ser.append_pair("after",bill);
    }
    build_okx_signed_get(creds,unix_ms,domain,
        &format!("/api/v5/account/bills-archive?{}",ser.finish()))
}

pub fn build_okx_account_config_request(
    creds:&OkxCredentials,unix_ms:u64,domain:OkxDomain
)->Result<SignedRequest,RestBuildError>{
    build_okx_signed_get(creds,unix_ms,domain,"/api/v5/account/config")
}
pub fn build_okx_balance_request(
    creds:&OkxCredentials,unix_ms:u64,domain:OkxDomain
)->Result<SignedRequest,RestBuildError>{
    build_okx_signed_get(creds,unix_ms,domain,"/api/v5/account/balance")
}
pub fn build_okx_positions_request(
    creds:&OkxCredentials,unix_ms:u64,domain:OkxDomain
)->Result<SignedRequest,RestBuildError>{
    build_okx_signed_get(creds,unix_ms,domain,"/api/v5/account/positions")
}
pub fn build_okx_pending_spot_orders_request(
    creds:&OkxCredentials,unix_ms:u64,domain:OkxDomain
)->Result<SignedRequest,RestBuildError>{
    build_okx_signed_get(creds,unix_ms,domain,"/api/v5/trade/orders-pending?instType=SPOT")
}

pub fn build_binance_position_mode_request(
    creds: &BinanceCredentials,
    unix_ms: u64,
    recv_window_ms: u64,
) -> Result<SignedRequest, RestBuildError> {
    if recv_window_ms == 0 || recv_window_ms > 60_000 {
        return Err(RestBuildError::InvalidRecvWindow);
    }
    let pairs = vec![
        ("timestamp", unix_ms.to_string()),
        ("recvWindow", recv_window_ms.to_string()),
    ];
    let unsigned = form_body(&pairs);
    let signature = hex_lower(&hmac_sha256(&creds.secret, unsigned.as_bytes())?);
    let query = format!("{unsigned}&signature={signature}");
    Ok(SignedRequest {
        venue: Venue::BinanceUsdm,
        method: HttpMethod::Get,
        url: format!("https://fapi.binance.com/fapi/v1/positionSide/dual?{query}"),
        headers: vec![("X-MBX-APIKEY".into(), creds.api_key.clone())],
        body: None,
        client_order_id: None,
        is_mutation: false,
    })
}

pub fn parse_binance_position_mode_http(
    status_code: u16,
    body: &str,
) -> BinancePositionMode {
    if !(200..300).contains(&status_code) {
        return BinancePositionMode::Unknown;
    }
    let value: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return BinancePositionMode::Unknown,
    };
    match value.get("dualSidePosition").and_then(Value::as_bool) {
        Some(false) => BinancePositionMode::OneWay,
        Some(true) => BinancePositionMode::Hedge,
        None => BinancePositionMode::Unknown,
    }
}

fn text(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| {
        x.as_str()
            .map(str::to_owned)
            .or_else(|| x.as_i64().map(|n| n.to_string()))
            .or_else(|| x.as_u64().map(|n| n.to_string()))
    })
}

fn lower(v: Option<String>) -> Option<String> {
    v.map(|x| x.to_ascii_lowercase())
}

pub const QUANTITY_SCALE_12: u128 = 1_000_000_000_000;

pub fn decimal_to_units_12(value: &str) -> Result<u128, RestBuildError> {
    let raw = value.trim();
    if raw.is_empty() || raw.starts_with('-') || raw.starts_with('+') {
        return Err(RestBuildError::InvalidQuantity);
    }
    let mut parts = raw.split('.');
    let whole = parts.next().ok_or(RestBuildError::InvalidQuantity)?;
    let frac = parts.next().unwrap_or("");
    if parts.next().is_some()
        || whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
        || frac.len() > 12
    {
        return Err(RestBuildError::InvalidQuantity);
    }
    let whole_num: u128 = whole.parse().map_err(|_| RestBuildError::InvalidQuantity)?;
    let mut frac_text = frac.to_string();
    while frac_text.len() < 12 {
        frac_text.push('0');
    }
    let frac_num: u128 = if frac_text.is_empty() {
        0
    } else {
        frac_text.parse().map_err(|_| RestBuildError::InvalidQuantity)?
    };
    whole_num
        .checked_mul(QUANTITY_SCALE_12)
        .and_then(|x| x.checked_add(frac_num))
        .ok_or(RestBuildError::InvalidQuantity)
}

pub fn to_lifecycle_mutation_observation(
    observation: &OrderObservation,
) -> Result<LifecycleMutationObservation, RestBuildError> {
    let filled_units = match observation.filled.as_deref() {
        Some(v) => decimal_to_units_12(v)?,
        None => 0,
    };
    Ok(LifecycleMutationObservation {
        class: observation.class.adapter_class(),
        terminal: observation.terminal,
        filled_units,
    })
}

/// R32A: classify a signed close response for native e18 authoritative state.
/// Any unverified identity, malformed quantity, or overfill becomes UNKNOWN.
/// In particular, R15/R16 quantity parsing is e12 and must not be used here.
pub fn to_native_close_observation_e18(
    observation:&OrderObservation,expected_client_id:&str,requested_e18:u128,
)->LifecycleMutationObservation{
    let unknown=||LifecycleMutationObservation{
        class:ResponseClass::Unknown,terminal:false,filled_units:0,
    };
    if expected_client_id.is_empty()||requested_e18==0{return unknown()}
    if observation.class==RestOutcomeClass::Unknown{return unknown()}
    if observation.client_order_id.as_deref()!=Some(expected_client_id){return unknown()}
    if observation.class==RestOutcomeClass::Rejected{
        if !observation.terminal{return unknown()}
        // A rejection without explicit zero-fill evidence is ambiguous.
        if observation.filled.as_deref()
            .and_then(|x|decimal_to_units_12(x).ok())!=Some(0){return unknown()}
        return LifecycleMutationObservation{
            class:ResponseClass::Rejected,terminal:true,filled_units:0,
        }
    }
    if !observation.terminal{
        // Placement ACK is not exchange fill truth.
        return LifecycleMutationObservation{
            class:ResponseClass::Acknowledged,terminal:false,filled_units:0,
        }
    }
    let Some(qty)=observation.filled.as_deref()else{return unknown()};
    let Ok(e12)=decimal_to_units_12(qty)else{return unknown()};
    let Some(e18)=e12.checked_mul(1_000_000)else{return unknown()};
    if e18>requested_e18{return unknown()}
    LifecycleMutationObservation{
        class:ResponseClass::Acknowledged,terminal:true,filled_units:e18,
    }
}

/// R32C strict native read-only reconciliation. A single "not found" reply
/// is NEVER proof that a possibly transmitted order did not execute.
/// Only an exact client ID, exchange order ID and terminal quantity can
/// resolve the pending native close. Quantity conversion e12 -> e18 is checked.
pub fn native_close_query_evidence_e18(
    observation:&OrderObservation,client_id:&str,requested_e18:u128,
)->Option<RecoveryEvidence>{
    if client_id.is_empty()||requested_e18==0{return None}
    if observation.client_order_id.as_deref()!=Some(client_id){return None}
    if observation.order_id.as_deref().map_or(true,str::is_empty){return None}
    if observation.class==RestOutcomeClass::Unknown{return None}
    if !observation.terminal{
        return if observation.class==RestOutcomeClass::Acknowledged{
            Some(RecoveryEvidence::StillOpen)
        }else{None}
    }
    let filled=decimal_to_units_12(observation.filled.as_deref()?).ok()?
        .checked_mul(1_000_000)?;
    if filled>requested_e18{return None}
    if observation.class==RestOutcomeClass::Rejected{
        return if filled==0 && observation.status.as_deref()==Some("rejected"){
            Some(RecoveryEvidence::ConfirmedRejected)
        }else{None}
    }
    // Only well-identified terminal exchange order observations.
    if !matches!(observation.status.as_deref(),
        Some("filled"|"canceled"|"cancelled"|"expired"|"mmp_canceled")){
        return None
    }
    Some(if filled==0{RecoveryEvidence::ConfirmedNoFill}
        else{RecoveryEvidence::ConfirmedFill{filled_units:filled,terminal:true}})
}

pub fn query_observation_to_recovery_evidence(
    observation: &OrderObservation,
) -> Result<RecoveryEvidence, RestBuildError> {
    // R32C: a single exchange NOT_FOUND is insufficient to exclude an order
    // that may have been accepted before the response was lost.
    if observation.reason == "BINANCE_ORDER_NOT_FOUND" {
        return Ok(RecoveryEvidence::Unknown);
    }
    match observation.class {
        RestOutcomeClass::Unknown => Ok(RecoveryEvidence::Unknown),
        RestOutcomeClass::Rejected => Err(RestBuildError::InsufficientRecoveryEvidence),
        RestOutcomeClass::Acknowledged if !observation.terminal => Ok(RecoveryEvidence::StillOpen),
        RestOutcomeClass::Acknowledged => {
            let filled = observation
                .filled
                .as_deref()
                .ok_or(RestBuildError::InsufficientRecoveryEvidence)?;
            let units = decimal_to_units_12(filled)?;
            if units == 0 {
                Ok(RecoveryEvidence::ConfirmedNoFill)
            } else {
                Ok(RecoveryEvidence::ConfirmedFill {
                    filled_units: units,
                    terminal: true,
                })
            }
        }
    }
}

pub fn classify_transport_failure() -> OrderObservation {
    OrderObservation {
        class: RestOutcomeClass::Unknown,
        terminal: false,
        filled: None,
        status: None,
        order_id: None,
        client_order_id: None,
        reason: "TRANSPORT_OUTCOME_UNKNOWN",
    }
}

pub fn parse_binance_order_http(status_code: u16, body: &str) -> OrderObservation {
    if status_code == 408 {
        return classify_transport_failure();
    }
    if status_code == 503
        && body.contains("Unknown error, please check your request or try again later.")
    {
        return OrderObservation {
            reason: "BINANCE_503_EXECUTION_UNKNOWN",
            ..classify_transport_failure()
        };
    }
    let value: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => {
            return OrderObservation {
                reason: "BINANCE_RESPONSE_PARSE_UNKNOWN",
                ..classify_transport_failure()
            }
        }
    };
    let error_code = value.get("code").and_then(Value::as_i64);
    if matches!(error_code, Some(-1000 | -1001 | -1006 | -1007)) {
        return OrderObservation {
            reason: "BINANCE_EXECUTION_STATUS_UNKNOWN",
            ..classify_transport_failure()
        };
    }
    if error_code == Some(-2013) {
        return OrderObservation {
            class: RestOutcomeClass::Rejected,
            terminal: true,
            filled: Some("0".into()),
            status: Some("not_found".into()),
            order_id: None,
            client_order_id: None,
            reason: "BINANCE_ORDER_NOT_FOUND",
        };
    }
    if !(200..300).contains(&status_code) {
        return OrderObservation {
            class: if status_code >= 500 {
                RestOutcomeClass::Unknown
            } else {
                RestOutcomeClass::Rejected
            },
            terminal: status_code < 500,
            filled: None,
            status: None,
            order_id: text(&value, "orderId"),
            client_order_id: text(&value, "clientOrderId"),
            reason: if status_code >= 500 {
                "BINANCE_SERVER_ERROR_UNKNOWN"
            } else {
                "BINANCE_HTTP_REJECTED"
            },
        };
    }
    if error_code.is_some_and(|x| x < 0) {
        return OrderObservation {
            class: RestOutcomeClass::Rejected,
            terminal: true,
            filled: None,
            status: None,
            order_id: text(&value, "orderId"),
            client_order_id: text(&value, "clientOrderId"),
            reason: "BINANCE_EXCHANGE_REJECTED",
        };
    }
    let status = lower(text(&value, "status"));
    let filled = text(&value, "executedQty").or_else(|| text(&value, "cumQty"));
    let terminal = matches!(
        status.as_deref(),
        Some("filled") | Some("canceled") | Some("cancelled") | Some("expired") | Some("rejected")
    );
    let class = match status.as_deref() {
        Some("rejected") | Some("expired") => RestOutcomeClass::Rejected,
        Some(_) => RestOutcomeClass::Acknowledged,
        None if text(&value, "orderId").is_some() => RestOutcomeClass::Acknowledged,
        None => RestOutcomeClass::Unknown,
    };
    OrderObservation {
        class,
        terminal,
        filled,
        status,
        order_id: text(&value, "orderId"),
        client_order_id: text(&value, "clientOrderId"),
        reason: match class {
            RestOutcomeClass::Acknowledged => "BINANCE_ORDER_OBSERVED",
            RestOutcomeClass::Rejected => "BINANCE_ORDER_REJECTED",
            RestOutcomeClass::Unknown => "BINANCE_ORDER_STATUS_UNKNOWN",
        },
    }
}

pub fn parse_okx_order_http(status_code: u16, body: &str, query_response: bool) -> OrderObservation {
    if status_code == 408 || status_code >= 500 {
        return OrderObservation {
            reason: "OKX_HTTP_OUTCOME_UNKNOWN",
            ..classify_transport_failure()
        };
    }
    let value: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => {
            return OrderObservation {
                reason: "OKX_RESPONSE_PARSE_UNKNOWN",
                ..classify_transport_failure()
            }
        }
    };
    let top_code = text(&value, "code").unwrap_or_default();
    if !(200..300).contains(&status_code) || top_code != "0" {
        return OrderObservation {
            class: RestOutcomeClass::Rejected,
            terminal: true,
            filled: None,
            status: None,
            order_id: None,
            client_order_id: None,
            reason: "OKX_HTTP_OR_TOPLEVEL_REJECTED",
        };
    }
    let row = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|x| x.first());
    let Some(row) = row else {
        return OrderObservation {
            reason: "OKX_EMPTY_DATA_UNKNOWN",
            ..classify_transport_failure()
        };
    };
    if let Some(code) = text(row, "sCode") {
        if code != "0" {
            return OrderObservation {
                class: RestOutcomeClass::Rejected,
                terminal: true,
                filled: None,
                status: lower(text(row, "state")),
                order_id: text(row, "ordId"),
                client_order_id: text(row, "clOrdId"),
                reason: "OKX_EXCHANGE_REJECTED",
            };
        }
    }
    if !query_response {
        return OrderObservation {
            class: RestOutcomeClass::Acknowledged,
            terminal: false,
            filled: None,
            status: None,
            order_id: text(row, "ordId"),
            client_order_id: text(row, "clOrdId"),
            reason: "OKX_PLACE_ACK_NOT_FILL_TRUTH",
        };
    }
    let status = lower(text(row, "state"));
    let filled = text(row, "accFillSz");
    let terminal = matches!(
        status.as_deref(),
        Some("filled") | Some("canceled") | Some("cancelled") | Some("mmp_canceled")
    );
    let class = match status.as_deref() {
        Some("filled") | Some("live") | Some("partially_filled") | Some("canceled")
        | Some("cancelled") | Some("mmp_canceled") => RestOutcomeClass::Acknowledged,
        Some(_) => RestOutcomeClass::Unknown,
        None => RestOutcomeClass::Unknown,
    };
    OrderObservation {
        class,
        terminal,
        filled,
        status,
        order_id: text(row, "ordId"),
        client_order_id: text(row, "clOrdId"),
        reason: match class {
            RestOutcomeClass::Acknowledged => "OKX_ORDER_OBSERVED",
            RestOutcomeClass::Rejected => "OKX_ORDER_REJECTED",
            RestOutcomeClass::Unknown => "OKX_ORDER_STATUS_UNKNOWN",
        },
    }
}


fn parse_u64_text(v:&Value,key:&str)->Option<u64>{text(v,key).and_then(|x|x.parse::<u64>().ok())}
fn econ_atoms(v:&Value,key:&str)->Result<Option<u128>,EconomicTruthError>{
    match text(v,key){Some(x) if x.trim().is_empty()=>Ok(None),Some(x)=>Ok(Some(econ_decimal_to_atoms(&x)?)),None=>Ok(None)}
}

fn economic_unknown(observation:OrderObservation,reason:&'static str)->OrderEconomicTruth{
    OrderEconomicTruth{class:EconomicTruthClass::Unknown,terminal:observation.terminal,
        cumulative_qty_atoms:observation.filled.as_deref().and_then(|x|econ_decimal_to_atoms(x).ok()),
        avg_price_atoms:None,cumulative_quote_atoms:None,status:observation.status,
        order_id:observation.order_id,client_order_id:observation.client_order_id,
        observed_fill_time_ms:None,reason}
}

pub fn parse_binance_economic_truth_http(status_code:u16,body:&str)->OrderEconomicTruth{
    let ordinary=parse_binance_order_http(status_code,body);
    if ordinary.class==RestOutcomeClass::Unknown{return economic_unknown(ordinary,"BINANCE_ORDER_STATUS_UNKNOWN")}
    if ordinary.class==RestOutcomeClass::Rejected{return OrderEconomicTruth{class:EconomicTruthClass::Rejected,terminal:ordinary.terminal,
        cumulative_qty_atoms:ordinary.filled.as_deref().and_then(|x|econ_decimal_to_atoms(x).ok()),avg_price_atoms:None,cumulative_quote_atoms:None,
        status:ordinary.status,order_id:ordinary.order_id,client_order_id:ordinary.client_order_id,observed_fill_time_ms:None,reason:ordinary.reason}}
    let v:Value=match serde_json::from_str(body){Ok(x)=>x,Err(_)=>return economic_unknown(ordinary,"BINANCE_ECON_PARSE_UNKNOWN")};
    let qty=match econ_atoms(&v,"executedQty"){Ok(v)=>v,Err(_)=>return economic_unknown(ordinary,"BINANCE_ECON_QTY_INVALID")};
    let avg=match econ_atoms(&v,"avgPrice"){Ok(v)=>v.filter(|x|*x>0),Err(_)=>return economic_unknown(ordinary,"BINANCE_ECON_AVG_INVALID")};
    let quote=match econ_atoms(&v,"cumQuote"){Ok(v)=>v,Err(_)=>return economic_unknown(ordinary,"BINANCE_ECON_QUOTE_INVALID")};
    if qty.is_none(){return economic_unknown(ordinary,"BINANCE_ECON_QTY_MISSING")}
    if qty.is_some_and(|x|x>0)&&avg.is_none(){return OrderEconomicTruth{class:EconomicTruthClass::Unknown,terminal:ordinary.terminal,cumulative_qty_atoms:qty,avg_price_atoms:None,cumulative_quote_atoms:quote,status:ordinary.status,order_id:ordinary.order_id,client_order_id:ordinary.client_order_id,observed_fill_time_ms:parse_u64_text(&v,"updateTime"),reason:"BINANCE_ECON_AVG_MISSING"}}
    OrderEconomicTruth{class:EconomicTruthClass::Known,terminal:ordinary.terminal,cumulative_qty_atoms:qty,avg_price_atoms:avg,cumulative_quote_atoms:quote,status:ordinary.status,order_id:ordinary.order_id,client_order_id:ordinary.client_order_id,observed_fill_time_ms:parse_u64_text(&v,"updateTime"),reason:"BINANCE_ECONOMIC_TRUTH"}
}

pub fn parse_okx_economic_truth_http(status_code:u16,body:&str)->OrderEconomicTruth{
    let ordinary=parse_okx_order_http(status_code,body,true);
    if ordinary.class==RestOutcomeClass::Unknown{return economic_unknown(ordinary,"OKX_ORDER_STATUS_UNKNOWN")}
    if ordinary.class==RestOutcomeClass::Rejected{return OrderEconomicTruth{class:EconomicTruthClass::Rejected,terminal:ordinary.terminal,
        cumulative_qty_atoms:ordinary.filled.as_deref().and_then(|x|econ_decimal_to_atoms(x).ok()),avg_price_atoms:None,cumulative_quote_atoms:None,
        status:ordinary.status,order_id:ordinary.order_id,client_order_id:ordinary.client_order_id,observed_fill_time_ms:None,reason:ordinary.reason}}
    let v:Value=match serde_json::from_str(body){Ok(x)=>x,Err(_)=>return economic_unknown(ordinary,"OKX_ECON_PARSE_UNKNOWN")};
    let row=match v.get("data").and_then(Value::as_array).and_then(|x|x.first()){Some(x)=>x,None=>return economic_unknown(ordinary,"OKX_ECON_EMPTY")};
    let qty=match econ_atoms(row,"accFillSz"){Ok(v)=>v,Err(_)=>return economic_unknown(ordinary,"OKX_ECON_QTY_INVALID")};
    let avg=match econ_atoms(row,"avgPx"){Ok(v)=>v.filter(|x|*x>0),Err(_)=>return economic_unknown(ordinary,"OKX_ECON_AVG_INVALID")};
    if qty.is_none(){return economic_unknown(ordinary,"OKX_ECON_QTY_MISSING")}
    if qty.is_some_and(|x|x>0)&&avg.is_none(){return OrderEconomicTruth{class:EconomicTruthClass::Unknown,terminal:ordinary.terminal,cumulative_qty_atoms:qty,avg_price_atoms:None,cumulative_quote_atoms:None,status:ordinary.status,order_id:ordinary.order_id,client_order_id:ordinary.client_order_id,observed_fill_time_ms:parse_u64_text(row,"fillTime"),reason:"OKX_ECON_AVG_MISSING"}}
    OrderEconomicTruth{class:EconomicTruthClass::Known,terminal:ordinary.terminal,cumulative_qty_atoms:qty,avg_price_atoms:avg,cumulative_quote_atoms:None,status:ordinary.status,order_id:ordinary.order_id,client_order_id:ordinary.client_order_id,observed_fill_time_ms:parse_u64_text(row,"fillTime"),reason:"OKX_ECONOMIC_TRUTH"}
}

pub fn apply_order_economic_truth(ledger:&mut LegFillLedger,truth:&OrderEconomicTruth)->Result<(),EconomicTruthError>{
    if truth.class!=EconomicTruthClass::Known{return Err(EconomicTruthError::InvalidResponse)}
    if let Some(cid)=&truth.client_order_id{if !cid.is_empty()&&cid!=&ledger.client_order_id{return Err(EconomicTruthError::IdentityMismatch)}}
    if let (Some(expected),Some(observed))=(&ledger.exchange_order_id,&truth.order_id){if expected!=observed{return Err(EconomicTruthError::IdentityMismatch)}}
    let qty=truth.cumulative_qty_atoms.ok_or(EconomicTruthError::InvalidResponse)?;
    if let (Some(avg),Some(quote))=(truth.avg_price_atoms,truth.cumulative_quote_atoms){
        let expected=avg.checked_mul(qty).ok_or(EconomicTruthError::Economic(EconError::Overflow))?/crate::fill_economics::SCALE_12;
        if expected.abs_diff(quote)>ECON_CUMQUOTE_TOLERANCE_ATOMS{return Err(EconomicTruthError::Economic(EconError::AverageMismatch))}
    }
    ledger.apply_cumulative_truth(qty,truth.avg_price_atoms,truth.terminal,truth.status.as_deref(),truth.order_id.as_deref(),truth.observed_fill_time_ms,DEFAULT_AVG_TOLERANCE_ATOMS)?;Ok(())
}

pub fn parse_okx_fill_events_http(status_code:u16,body:&str,expected_order_id:&str,base_currency:&str,quote_currency:&str)->Result<Vec<FillEvent>,EconomicTruthError>{
    if !(200..300).contains(&status_code){return Err(EconomicTruthError::InvalidResponse)}
    let v:Value=serde_json::from_str(body).map_err(|_|EconomicTruthError::InvalidResponse)?;
    if text(&v,"code").unwrap_or_default()!="0"{return Err(EconomicTruthError::InvalidResponse)}
    let rows=v.get("data").and_then(Value::as_array).ok_or(EconomicTruthError::InvalidResponse)?;let mut out=Vec::new();
    for row in rows{
        if text(row,"ordId").as_deref()!=Some(expected_order_id){continue}
        let id=text(row,"tradeId").filter(|x|!x.is_empty()).ok_or(EconomicTruthError::InvalidResponse)?;
        let price=econ_atoms(row,"fillPx")?.filter(|x|*x>0).ok_or(EconomicTruthError::InvalidResponse)?;
        let qty=econ_atoms(row,"fillSz")?.filter(|x|*x>0).ok_or(EconomicTruthError::InvalidResponse)?;
        let fee_ccy=text(row,"feeCcy");let fee_cost=match (text(row,"fee"),fee_ccy.as_deref()){
            (Some(raw),Some(ccy)) if ccy.eq_ignore_ascii_case(quote_currency)=>Some(econ_signed_decimal_to_atoms(&raw)?.checked_neg().ok_or(EconomicTruthError::Economic(EconError::Overflow))?),
            (Some(raw),Some(ccy)) if ccy.eq_ignore_ascii_case(base_currency)=>{
                let native_cost=econ_signed_decimal_to_atoms(&raw)?.checked_neg().ok_or(EconomicTruthError::Economic(EconError::Overflow))?;
                Some(native_cost.checked_mul(price as i128).and_then(|x|x.checked_div(crate::fill_economics::SCALE_12 as i128)).ok_or(EconomicTruthError::Economic(EconError::Overflow))?)
            },
            _=>None};
        out.push(FillEvent{fill_id:id,price_atoms:price,qty_atoms:qty,fee_cost_quote_atoms:fee_cost,fee_currency:fee_ccy,trade_time_ms:parse_u64_text(row,"fillTime")});
    }
    Ok(out)
}

pub fn parse_binance_fill_events_http(status_code:u16,body:&str,expected_order_id:&str,quote_currency:&str)->Result<Vec<FillEvent>,EconomicTruthError>{
    if !(200..300).contains(&status_code){return Err(EconomicTruthError::InvalidResponse)}
    let v:Value=serde_json::from_str(body).map_err(|_|EconomicTruthError::InvalidResponse)?;
    let rows=v.as_array().ok_or(EconomicTruthError::InvalidResponse)?;let mut out=Vec::new();
    for row in rows{
        if text(row,"orderId").as_deref()!=Some(expected_order_id){continue}
        let id=text(row,"id").filter(|x|!x.is_empty()).ok_or(EconomicTruthError::InvalidResponse)?;
        let price=econ_atoms(row,"price")?.filter(|x|*x>0).ok_or(EconomicTruthError::InvalidResponse)?;
        let qty=econ_atoms(row,"qty")?.filter(|x|*x>0).ok_or(EconomicTruthError::InvalidResponse)?;
        let fee_ccy=text(row,"commissionAsset");let fee_cost=match (text(row,"commission"),fee_ccy.as_deref()){
            (Some(raw),Some(ccy)) if ccy.eq_ignore_ascii_case(quote_currency)=>Some(econ_signed_decimal_to_atoms(&raw)?),
            _=>None};
        out.push(FillEvent{fill_id:id,price_atoms:price,qty_atoms:qty,fee_cost_quote_atoms:fee_cost,fee_currency:fee_ccy,trade_time_ms:parse_u64_text(row,"time")});
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TS: u64 = 1_700_000_000_123;

    fn okx_creds() -> OkxCredentials {
        OkxCredentials::new("TEST_OKX_KEY", "TEST_OKX_SECRET", "TEST_OKX_PASS").unwrap()
    }
    fn binance_creds() -> BinanceCredentials {
        BinanceCredentials::new("TEST_BIN_KEY", "TEST_BIN_SECRET").unwrap()
    }
    fn action(leg: Leg, id: &str) -> ExecAction {
        ExecAction::NetworkMutation {
            intent_id: id.into(),
            leg,
        }
    }
    fn okx_intent(id: &str, side: &str, amount: &str) -> OrderIntent {
        OrderIntent {
            venue: Venue::OkxSpot,
            symbol: "BNT/USDT".into(),
            side: side.into(),
            amount: amount.into(),
            client_id: id.into(),
        }
    }
    fn binance_intent(id: &str, side: &str, amount: &str) -> OrderIntent {
        OrderIntent {
            venue: Venue::BinanceUsdm,
            symbol: "BNT/USDT:USDT".into(),
            side: side.into(),
            amount: amount.into(),
            client_id: id.into(),
        }
    }

    #[test]
    fn p003_binance_income_window_builder_omits_optional_page(){
        let req=build_binance_income_time_window_request(
          &binance_creds(),TS,5000,TS-86_400_000,TS-1,1000).unwrap();
        assert!(!req.is_mutation);
        assert!(matches!(req.method,HttpMethod::Get));
        assert!(req.url.contains("/fapi/v1/income?"));
        assert!(req.url.contains("startTime="));
        assert!(req.url.contains("endTime="));
        assert!(req.url.contains("limit=1000"));
        assert!(!req.url.contains("page="));
        assert!(build_binance_income_time_window_request(
          &binance_creds(),TS,5000,TS-86_400_000,TS-1,1001).is_err());
        assert!(build_binance_income_time_window_request(
          &binance_creds(),TS,5000,TS,TS+1,100).is_err());
    }

    #[test]
    fn r32a_native_e18_close_bridge_preserves_quantity_and_identity(){
        let id="sab00000000000000000000000000061";
        let ack=OrderObservation{
            class:RestOutcomeClass::Acknowledged,terminal:true,
            filled:Some("0.6".into()),status:Some("filled".into()),
            order_id:Some("22".into()),client_order_id:Some(id.into()),
            reason:"BINANCE_ORDER_OBSERVED",
        };
        let old=to_lifecycle_mutation_observation(&ack).unwrap();
        let native=to_native_close_observation_e18(&ack,id,1_000_000_000_000_000_000);
        assert_eq!(old.filled_units,600_000_000_000);
        assert_eq!(native.filled_units,600_000_000_000_000_000);
        assert_eq!(native.class,ResponseClass::Acknowledged);
        assert!(native.terminal);
        for bad in [
            OrderObservation{client_order_id:Some("WRONG".into()),..ack.clone()},
            OrderObservation{client_order_id:None,..ack.clone()},
            OrderObservation{filled:Some("1.01".into()),..ack.clone()},
            OrderObservation{filled:Some("0.1234567890123".into()),..ack.clone()},
            OrderObservation{filled:None,..ack.clone()},
        ]{
            let v=to_native_close_observation_e18(&bad,id,1_000_000_000_000_000_000);
            assert_eq!(v.class,ResponseClass::Unknown);
            assert_eq!(v.filled_units,0);
            assert!(!v.terminal);
        }
        let pending=OrderObservation{terminal:false,filled:None,..ack.clone()};
        let v=to_native_close_observation_e18(&pending,id,1_000_000_000_000_000_000);
        assert_eq!(v.class,ResponseClass::Acknowledged);
        assert!(!v.terminal);
        assert_eq!(v.filled_units,0);
        let missing=OrderObservation{
            class:RestOutcomeClass::Unknown,terminal:false,filled:None,..ack.clone()
        };
        assert_eq!(to_native_close_observation_e18(
            &missing,id,1_000_000_000_000_000_000).class,ResponseClass::Unknown);
        let rejected=OrderObservation{
            class:RestOutcomeClass::Rejected,terminal:true,filled:Some("0".into()),..ack.clone()
        };
        assert_eq!(to_native_close_observation_e18(
            &rejected,id,1_000_000_000_000_000_000).class,ResponseClass::Rejected);
        let missing_reject_qty=OrderObservation{
            class:RestOutcomeClass::Rejected,terminal:true,filled:None,..ack.clone()
        };
        assert_eq!(to_native_close_observation_e18(
            &missing_reject_qty,id,1_000_000_000_000_000_000).class,ResponseClass::Unknown);
        let bad_rejected=OrderObservation{
            class:RestOutcomeClass::Rejected,terminal:true,..ack.clone()
        };
        assert_eq!(to_native_close_observation_e18(
            &bad_rejected,id,1_000_000_000_000_000_000).class,ResponseClass::Unknown);
    }

    #[test]
    fn timestamp_matches_ccxt_iso8601() {
        assert_eq!(okx_timestamp_from_unix_ms(TS), "2023-11-14T22:13:20.123Z");
    }

    #[test]
    fn okx_fok_signing_matches_ccxt_4_5_39_oracle() {
        let id = "sab00000000000000000000000000001";
        let req = build_okx_order(
            &okx_intent(id,"buy","10"),OrderKind::AggressiveLimitFok,
            Some("0.75"),&okx_creds(),TS,OkxDomain::Global)
        .unwrap();
        assert_eq!(req.url, "https://www.okx.com/api/v5/trade/order");
        assert_eq!(
            req.body.as_deref(),
            Some("{\"instId\":\"BNT-USDT\",\"tdMode\":\"cash\",\"clOrdId\":\"sab00000000000000000000000000001\",\"side\":\"buy\",\"ordType\":\"fok\",\"px\":\"0.75\",\"sz\":\"10\",\"tgtCcy\":\"base_ccy\"}")
        );
        assert_eq!(
            header(&req, "OK-ACCESS-TIMESTAMP"),
            Some("2023-11-14T22:13:20.123Z")
        );
        assert_eq!(
            header(&req, "OK-ACCESS-SIGN"),
            Some("1EbIwM67BtViSl7Cz+il4R9SSViyKqtpN7nMznoeDsE=")
        );
    }

    #[test]
    fn okx_spot_sell_market_matches_ccxt_oracle() {
        let id = "sab00000000000000000000000000002";
        let req = build_signed_order_from_network_boundary(
            &action(Leg::Spot, "i2"),
            &okx_intent(id, "sell", "10"),
            OrderKind::Market,
            false,
            None,
            7,
            7,
            CredentialsRef::Okx(&okx_creds()),
            TS,
            OkxDomain::Global,
            BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
        )
        .unwrap();
        assert_eq!(
            req.body.as_deref(),
            Some("{\"instId\":\"BNT-USDT\",\"tdMode\":\"cash\",\"clOrdId\":\"sab00000000000000000000000000002\",\"side\":\"sell\",\"ordType\":\"market\",\"sz\":\"10\",\"tgtCcy\":\"base_ccy\"}")
        );
        assert_eq!(
            header(&req, "OK-ACCESS-SIGN"),
            Some("kBvw/JPxFtHtfFZ2AbJLuhxNldIn8RVj79YqZXRSk8s=")
        );
    }

    #[test]
    fn binance_entry_market_matches_ccxt_oracle() {
        let id = "sab00000000000000000000000000003";
        let req = build_binance_order(
            &binance_intent(id,"sell","10"),false,
            &binance_creds(),TS,BINANCE_DEFAULT_RECV_WINDOW_MS,BinancePositionMode::OneWay)
        .unwrap();
        assert_eq!(
            req.body.as_deref(),
            Some("timestamp=1700000000123&symbol=BNTUSDT&side=SELL&type=MARKET&quantity=10&newClientOrderId=sab00000000000000000000000000003&newOrderRespType=RESULT&recvWindow=10000&signature=c585ba5c38cfdf2463f4b11e9a54614658146f6d4a4f8eb03d398e4507612f1c")
        );
        assert_eq!(header(&req, "X-MBX-APIKEY"), Some("TEST_BIN_KEY"));
    }

    #[test]
    fn binance_reduce_only_close_matches_ccxt_oracle() {
        let id = "sab00000000000000000000000000004";
        let req = build_signed_order_from_network_boundary(
            &action(Leg::Futures, "i4"),
            &binance_intent(id, "buy", "10"),
            OrderKind::Market,
            true,
            None,
            7,
            7,
            CredentialsRef::Binance(&binance_creds()),
            TS,
            OkxDomain::Global,
            BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
        )
        .unwrap();
        assert_eq!(
            req.body.as_deref(),
            Some("timestamp=1700000000123&symbol=BNTUSDT&side=BUY&type=MARKET&quantity=10&newClientOrderId=sab00000000000000000000000000004&newOrderRespType=RESULT&reduceOnly=true&recvWindow=10000&signature=b5e05b036d6548718b2911ea16f46fcb5e4561503224556ac43c48e269d74a29")
        );
    }

    #[test]
    fn okx_query_signature_matches_ccxt_contract() {
        let req = build_okx_query_by_client_id(
            &okx_creds(),
            TS,
            OkxDomain::Global,
            "BNT/USDT",
            "sab00000000000000000000000000001",
        )
        .unwrap();
        assert_eq!(
            req.url,
            "https://www.okx.com/api/v5/trade/order?instId=BNT-USDT&clOrdId=sab00000000000000000000000000001"
        );
        assert_eq!(
            header(&req, "OK-ACCESS-SIGN"),
            Some("HqyGAzj+c1PuN8ofeoclmuejjgR8vfwVx0RPIhyZtHQ=")
        );
    }

    #[test]
    fn binance_query_signature_matches_ccxt_contract() {
        let req = build_binance_query_by_client_id(
            &binance_creds(),
            TS,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
            "BNT/USDT:USDT",
            "sab00000000000000000000000000003",
        )
        .unwrap();
        assert_eq!(
            req.url,
            "https://fapi.binance.com/fapi/v1/order?timestamp=1700000000123&symbol=BNTUSDT&origClientOrderId=sab00000000000000000000000000003&recvWindow=10000&signature=35e66fb0901b83eb32b7d4efc737e3d64fed8d8039ee8672ec778f4ee690fe8b"
        );
    }

    #[test]
    fn binance_position_mode_signed_get_contract() {
        let req = build_binance_position_mode_request(
            &binance_creds(),
            TS,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
        )
        .unwrap();
        assert_eq!(
            req.url,
            "https://fapi.binance.com/fapi/v1/positionSide/dual?timestamp=1700000000123&recvWindow=10000&signature=b1a8c30046f2ab925bc03a77227f85749a16fa43adb80b0318562f3f4fedecd3"
        );
        assert_eq!(header(&req, "X-MBX-APIKEY"), Some("TEST_BIN_KEY"));
        assert!(!req.is_mutation);
    }

    #[test]
    fn r26_private_truth_builders_are_read_only_signed_gets() {
        let bc=build_binance_account_config_request(&binance_creds(),TS,BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        let bb=build_binance_balance_v3_request(&binance_creds(),TS,BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        let bp=build_binance_position_risk_v3_request(&binance_creds(),TS,BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        let bo=build_binance_open_orders_request(&binance_creds(),TS,BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        for (req,path) in [(&bc,"/fapi/v1/accountConfig"),(&bb,"/fapi/v3/balance"),(&bp,"/fapi/v3/positionRisk"),(&bo,"/fapi/v1/openOrders")] {
            assert_eq!(req.method,HttpMethod::Get);assert!(!req.is_mutation);assert!(req.body.is_none());
            assert!(req.url.starts_with(&format!("https://fapi.binance.com{path}?")));
            assert!(req.url.contains("timestamp=1700000000123"));assert!(req.url.contains("signature="));
            assert_eq!(header(req,"X-MBX-APIKEY"),Some("TEST_BIN_KEY"));
        }
        let oc=build_okx_account_config_request(&okx_creds(),TS,OkxDomain::Global).unwrap();
        let ob=build_okx_balance_request(&okx_creds(),TS,OkxDomain::Global).unwrap();
        let op=build_okx_positions_request(&okx_creds(),TS,OkxDomain::Global).unwrap();
        let oo=build_okx_pending_spot_orders_request(&okx_creds(),TS,OkxDomain::Global).unwrap();
        assert_eq!(oc.url,"https://www.okx.com/api/v5/account/config");
        assert_eq!(ob.url,"https://www.okx.com/api/v5/account/balance");
        assert_eq!(op.url,"https://www.okx.com/api/v5/account/positions");
        assert_eq!(oo.url,"https://www.okx.com/api/v5/trade/orders-pending?instType=SPOT");
        for req in [&oc,&ob,&op,&oo] {
            assert_eq!(req.method,HttpMethod::Get);assert!(!req.is_mutation);assert!(req.body.is_none());
            assert_eq!(header(req,"OK-ACCESS-KEY"),Some("TEST_OKX_KEY"));
            assert!(header(req,"OK-ACCESS-SIGN").is_some());
        }
    }

    #[test]
    fn okx_account_config_get_signature_matches_oracle() {
        let req=build_okx_account_config_request(&okx_creds(),TS,OkxDomain::Global).unwrap();
        assert_eq!(req.url,"https://www.okx.com/api/v5/account/config");
        assert_eq!(header(&req,"OK-ACCESS-TIMESTAMP"),Some("2023-11-14T22:13:20.123Z"));
        assert_eq!(header(&req,"OK-ACCESS-SIGN"),Some("OVFA+mtyqOmD+2aPciQnxnPTyKWGxDtMIXZW39Sac1o="));
        assert!(!req.is_mutation);
    }

    #[test]
    fn binance_position_mode_parser_is_fail_closed() {
        assert_eq!(
            parse_binance_position_mode_http(200, r#"{"dualSidePosition":false}"#),
            BinancePositionMode::OneWay
        );
        assert_eq!(
            parse_binance_position_mode_http(200, r#"{"dualSidePosition":true}"#),
            BinancePositionMode::Hedge
        );
        assert_eq!(
            parse_binance_position_mode_http(500, "{}"),
            BinancePositionMode::Unknown
        );
        assert_eq!(
            parse_binance_position_mode_http(200, "{bad"),
            BinancePositionMode::Unknown
        );
    }

    #[test]
    fn signing_requires_r14_network_boundary() {
        let id = "sab00000000000000000000000000003";
        let err = build_signed_order_from_network_boundary(
            &ExecAction::PersistPrepare {
                intent_id: "i".into(),
                leg: Leg::Futures,
            },
            &binance_intent(id, "sell", "10"),
            OrderKind::Market,
            false,
            None,
            7,
            7,
            CredentialsRef::Binance(&binance_creds()),
            TS,
            OkxDomain::Global,
            BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
        )
        .unwrap_err();
        assert_eq!(err, RestBuildError::InvalidBoundaryAction);
    }

    #[test]
    fn signer_revalidates_safety_generation() {
        let id = "sab00000000000000000000000000003";
        let err = build_signed_order_from_network_boundary(
            &action(Leg::Futures, "i"),
            &binance_intent(id, "sell", "10"),
            OrderKind::Market,
            false,
            None,
            7,
            8,
            CredentialsRef::Binance(&binance_creds()),
            TS,
            OkxDomain::Global,
            BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
        )
        .unwrap_err();
        assert_eq!(err, RestBuildError::StaleSafety);
    }

    #[test]
    fn wrong_leg_or_credentials_fail_closed() {
        let id = "sab00000000000000000000000000003";
        assert_eq!(
            build_signed_order_from_network_boundary(
                &action(Leg::Spot, "i"),
                &binance_intent(id, "sell", "10"),
                OrderKind::Market,
                false,
                None,
                7,
                7,
                CredentialsRef::Binance(&binance_creds()),
                TS,
                OkxDomain::Global,
                BinancePositionMode::OneWay,
                BINANCE_DEFAULT_RECV_WINDOW_MS,
            )
            .unwrap_err(),
            RestBuildError::InvalidBoundaryAction
        );
        assert_eq!(
            build_signed_order_from_network_boundary(
                &action(Leg::Futures, "i"),
                &binance_intent(id, "sell", "10"),
                OrderKind::Market,
                false,
                None,
                7,
                7,
                CredentialsRef::Okx(&okx_creds()),
                TS,
                OkxDomain::Global,
                BinancePositionMode::OneWay,
                BINANCE_DEFAULT_RECV_WINDOW_MS,
            )
            .unwrap_err(),
            RestBuildError::NativeEntryPermitRequired
        );
    }

    #[test]
    fn hedge_mode_reduce_only_is_blocked_before_http() {
        let id = "sab00000000000000000000000000004";
        assert_eq!(
            build_signed_order_from_network_boundary(
                &action(Leg::Futures, "i"),
                &binance_intent(id, "buy", "10"),
                OrderKind::Market,
                true,
                None,
                7,
                7,
                CredentialsRef::Binance(&binance_creds()),
                TS,
                OkxDomain::Global,
                BinancePositionMode::Hedge,
                BINANCE_DEFAULT_RECV_WINDOW_MS,
            )
            .unwrap_err(),
            RestBuildError::UnsupportedBinancePositionMode
        );
    }

    #[test]
    fn hedge_mode_entry_is_also_blocked_without_position_side_contract() {
        let id = "sab00000000000000000000000000003";
        assert_eq!(
            build_signed_order_from_network_boundary(
                &action(Leg::Futures, "i"),
                &binance_intent(id, "sell", "10"),
                OrderKind::Market,
                false,
                None,
                7,
                7,
                CredentialsRef::Binance(&binance_creds()),
                TS,
                OkxDomain::Global,
                BinancePositionMode::Hedge,
                BINANCE_DEFAULT_RECV_WINDOW_MS,
            )
            .unwrap_err(),
            RestBuildError::NativeEntryPermitRequired
        );
    }

    #[test]
    fn unknown_position_mode_fails_closed() {
        let id = "sab00000000000000000000000000003";
        assert_eq!(
            build_signed_order_from_network_boundary(
                &action(Leg::Futures, "i"),
                &binance_intent(id, "sell", "10"),
                OrderKind::Market,
                false,
                None,
                7,
                7,
                CredentialsRef::Binance(&binance_creds()),
                TS,
                OkxDomain::Global,
                BinancePositionMode::Unknown,
                BINANCE_DEFAULT_RECV_WINDOW_MS,
            )
            .unwrap_err(),
            RestBuildError::NativeEntryPermitRequired
        );
    }

    #[test]
    fn recv_window_is_bounded_by_binance_contract() {
        let id = "sab00000000000000000000000000003";
        for bad in [0, 60_001] {
            assert_eq!(
                build_signed_order_from_network_boundary(
                    &action(Leg::Futures, "i"),
                    &binance_intent(id, "sell", "10"),
                    OrderKind::Market,
                    false,
                    None,
                    7,
                    7,
                    CredentialsRef::Binance(&binance_creds()),
                    TS,
                    OkxDomain::Global,
                    BinancePositionMode::OneWay,
                    bad,
                )
                .unwrap_err(),
                RestBuildError::NativeEntryPermitRequired
            );
        }
    }

    #[test]
    fn credentials_are_not_exposed_by_request_debug() {
        let id = "sab00000000000000000000000000003";
        let req = build_signed_order_from_network_boundary(
            &action(Leg::Futures, "i"),
            &binance_intent(id, "buy", "10"),
            OrderKind::Market,
            true,
            None,
            7,
            7,
            CredentialsRef::Binance(&binance_creds()),
            TS,
            OkxDomain::Global,
            BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
        )
        .unwrap();
        let dbg = format!("{req:?}");
        assert!(!dbg.contains("TEST_BIN_KEY"));
        assert!(!dbg.contains("signature="));
    }

    #[test]
    fn binance_503_execution_unknown_is_not_rejected() {
        let r = parse_binance_order_http(
            503,
            r#"{"code":-1000,"msg":"Unknown error, please check your request or try again later."}"#,
        );
        assert_eq!(r.class, RestOutcomeClass::Unknown);
        assert_eq!(r.reason, "BINANCE_503_EXECUTION_UNKNOWN");
    }

    #[test]
    fn binance_filled_result_is_terminal_ack() {
        let r = parse_binance_order_http(
            200,
            r#"{"orderId":123,"clientOrderId":"c","status":"FILLED","executedQty":"10"}"#,
        );
        assert_eq!(r.class, RestOutcomeClass::Acknowledged);
        assert!(r.terminal);
        assert_eq!(r.filled.as_deref(), Some("10"));
    }

    #[test]
    fn binance_sender_error_is_terminal_reject() {
        let r = parse_binance_order_http(400, r#"{"code":-1121,"msg":"Invalid symbol."}"#);
        assert_eq!(r.class, RestOutcomeClass::Rejected);
        assert!(r.terminal);
    }

    #[test]
    fn binance_unparseable_or_5xx_is_unknown() {
        assert_eq!(
            parse_binance_order_http(200, "{bad").class,
            RestOutcomeClass::Unknown
        );
        assert_eq!(
            parse_binance_order_http(500, r#"{"code":-1000}"#).class,
            RestOutcomeClass::Unknown
        );
    }

    #[test]
    fn okx_place_success_is_ack_not_fill_truth() {
        let r = parse_okx_order_http(
            200,
            r#"{"code":"0","data":[{"clOrdId":"c","ordId":"123","sCode":"0","sMsg":""}],"msg":""}"#,
            false,
        );
        assert_eq!(r.class, RestOutcomeClass::Acknowledged);
        assert!(!r.terminal);
        assert_eq!(r.reason, "OKX_PLACE_ACK_NOT_FILL_TRUTH");
    }

    #[test]
    fn okx_query_filled_is_terminal_truth() {
        let r = parse_okx_order_http(
            200,
            r#"{"code":"0","data":[{"clOrdId":"c","ordId":"123","state":"filled","accFillSz":"10"}],"msg":""}"#,
            true,
        );
        assert_eq!(r.class, RestOutcomeClass::Acknowledged);
        assert!(r.terminal);
        assert_eq!(r.filled.as_deref(), Some("10"));
    }

    #[test]
    fn okx_exchange_reject_is_terminal() {
        let r = parse_okx_order_http(
            200,
            r#"{"code":"0","data":[{"clOrdId":"c","ordId":"","sCode":"51000","sMsg":"bad"}],"msg":""}"#,
            false,
        );
        assert_eq!(r.class, RestOutcomeClass::Rejected);
        assert!(r.terminal);
    }

    #[test]
    fn okx_timeout_or_bad_json_is_unknown() {
        assert_eq!(
            parse_okx_order_http(408, "{}", false).class,
            RestOutcomeClass::Unknown
        );
        assert_eq!(
            parse_okx_order_http(200, "{bad", false).class,
            RestOutcomeClass::Unknown
        );
    }

    #[test]
    fn exact_decimal_quantity_bridge_uses_twelve_decimal_scale() {
        assert_eq!(decimal_to_units_12("10").unwrap(), 10 * QUANTITY_SCALE_12);
        assert_eq!(decimal_to_units_12("0.000000000001").unwrap(), 1);
        assert_eq!(decimal_to_units_12("1.25").unwrap(), 1_250_000_000_000);
        assert_eq!(
            decimal_to_units_12("0.0000000000001"),
            Err(RestBuildError::InvalidQuantity)
        );
    }

    #[test]
    fn terminal_query_bridge_distinguishes_fill_no_fill_and_still_open() {
        let filled = OrderObservation {
            class: RestOutcomeClass::Acknowledged,
            terminal: true,
            filled: Some("0.5".into()),
            status: Some("filled".into()),
            order_id: Some("1".into()),
            client_order_id: Some("c".into()),
            reason: "OKX_ORDER_OBSERVED",
        };
        assert_eq!(
            query_observation_to_recovery_evidence(&filled).unwrap(),
            RecoveryEvidence::ConfirmedFill {
                filled_units: 500_000_000_000,
                terminal: true
            }
        );
        let no_fill = OrderObservation {
            filled: Some("0".into()),
            status: Some("canceled".into()),
            ..filled.clone()
        };
        assert_eq!(
            query_observation_to_recovery_evidence(&no_fill).unwrap(),
            RecoveryEvidence::ConfirmedNoFill
        );
        let live = OrderObservation {
            terminal: false,
            filled: Some("0".into()),
            status: Some("live".into()),
            ..filled
        };
        assert_eq!(
            query_observation_to_recovery_evidence(&live).unwrap(),
            RecoveryEvidence::StillOpen
        );
    }

    #[test]
    fn binance_timeout_and_single_not_found_are_unknown() {
        for code in [-1000, -1001, -1006, -1007] {
            let body = format!(r#"{{"code":{code},"msg":"x"}}"#);
            assert_eq!(
                parse_binance_order_http(400, &body).class,
                RestOutcomeClass::Unknown
            );
        }
        let missing =
            parse_binance_order_http(400, r#"{"code":-2013,"msg":"Order does not exist."}"#);
        assert_eq!(missing.reason, "BINANCE_ORDER_NOT_FOUND");
        assert_eq!(
            query_observation_to_recovery_evidence(&missing).unwrap(),
            RecoveryEvidence::Unknown
        );
    }

    #[test]
    fn placement_observation_maps_into_r15_without_float_quantity() {
        let rest = parse_binance_order_http(
            200,
            r#"{"orderId":123,"clientOrderId":"c","status":"FILLED","executedQty":"0.123456789012"}"#,
        );
        let lifecycle = to_lifecycle_mutation_observation(&rest).unwrap();
        assert_eq!(lifecycle.class, ResponseClass::Acknowledged);
        assert!(lifecycle.terminal);
        assert_eq!(lifecycle.filled_units, 123_456_789_012);
    }

    #[test]
    fn r15_durable_close_plan_reaches_r16_signer_only_after_r14_gate() {
        use crate::position_lifecycle::{DurableLifecycleStore, PositionLifecycle};
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};

        let root = std::env::temp_dir().join(format!(
            "spotarb-r16-link-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let lifecycle_store = DurableLifecycleStore::open(root.join("lifecycle.json")).unwrap();
        let mut gate = DurableMutationGate::open(root.join("intents.json")).unwrap();
        let mut lifecycle = PositionLifecycle::new_hedged(10, 7).unwrap();
        lifecycle.start_close().unwrap();
        lifecycle_store.persist(&lifecycle).unwrap();
        let plan = lifecycle
            .prepare_close_mutation(
                &lifecycle_store,
                &mut gate,
                "close-fut-1",
                "sab00000000000000000000000000004",
                "BNT/USDT:USDT",
                7,
                7,
            )
            .unwrap();
        let signed = build_signed_order_from_network_boundary(
            &plan.network_action,
            &plan.intent,
            plan.kind,
            plan.reduce_only,
            None,
            7,
            7,
            CredentialsRef::Binance(&binance_creds()),
            TS,
            OkxDomain::Global,
            BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
        )
        .unwrap();
        assert!(signed
            .body
            .as_deref()
            .unwrap()
            .contains("side=BUY&type=MARKET"));
        assert!(signed.body.as_deref().unwrap().contains("reduceOnly=true"));
        assert_eq!(
            gate.store.load().unwrap()[0].state,
            crate::execution::IntentState::Prepared
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn empty_credentials_fail_closed() {
        assert!(matches!(
            OkxCredentials::new("", "x", "y"),
            Err(RestBuildError::MissingCredential)
        ));
        assert!(matches!(
            BinanceCredentials::new("x", ""),
            Err(RestBuildError::MissingCredential)
        ));
    }
}


#[cfg(test)]
mod r19_economic_tests {
    use super::*;
    use crate::fill_economics::{atoms_to_string, LegFillLedger, SCALE_12};

    fn okx_creds() -> OkxCredentials { OkxCredentials::new("TEST_OKX_KEY","TEST_OKX_SECRET","TEST_OKX_PASS").unwrap() }
    fn bn_creds() -> BinanceCredentials { BinanceCredentials::new("TEST_BIN_KEY","TEST_BIN_SECRET").unwrap() }

    #[test]
    fn okx_cumulative_avg_truth_bridges_into_ledger() {
        let body=r#"{"code":"0","data":[{"ordId":"o1","clOrdId":"c1","state":"filled","accFillSz":"10","avgPx":"100.08","fillTime":"1700000000001"}],"msg":""}"#;
        let truth=parse_okx_economic_truth_http(200,body);
        assert_eq!(truth.class,EconomicTruthClass::Known);
        let mut l=LegFillLedger::new("okx","TEST-USDT","buy","i1","c1",10*SCALE_12).unwrap();
        apply_order_economic_truth(&mut l,&truth).unwrap();
        assert_eq!(l.filled_qty_atoms(),10*SCALE_12);
        assert_eq!(atoms_to_string(l.vwap_atoms().unwrap()),"100.08");
        assert!(l.terminal);
    }

    #[test]
    fn binance_cumulative_avg_truth_bridges_into_ledger() {
        let body=r#"{"orderId":22,"clientOrderId":"c2","status":"FILLED","executedQty":"10","avgPrice":"100.43","cumQuote":"1004.3","updateTime":1700000000002}"#;
        let truth=parse_binance_economic_truth_http(200,body);
        assert_eq!(truth.class,EconomicTruthClass::Known);
        assert_eq!(truth.cumulative_quote_atoms,Some(econ_decimal_to_atoms("1004.3").unwrap()));
        let mut l=LegFillLedger::new("binance","TESTUSDT","sell","i2","c2",10*SCALE_12).unwrap();
        apply_order_economic_truth(&mut l,&truth).unwrap();
        assert_eq!(atoms_to_string(l.vwap_atoms().unwrap()),"100.43");
    }

    #[test]
    fn missing_average_with_nonzero_fill_is_unknown() {
        let b=parse_binance_economic_truth_http(200,r#"{"orderId":1,"clientOrderId":"c","status":"FILLED","executedQty":"1","avgPrice":"0"}"#);
        assert_eq!(b.class,EconomicTruthClass::Unknown);
        let o=parse_okx_economic_truth_http(200,r#"{"code":"0","data":[{"ordId":"1","clOrdId":"c","state":"filled","accFillSz":"1","avgPx":"","fillTime":"1"}]}"#);
        assert_eq!(o.class,EconomicTruthClass::Unknown);
    }

    #[test]
    fn malformed_numeric_economic_fields_are_unknown() {
        let b=parse_binance_economic_truth_http(200,r#"{"orderId":1,"clientOrderId":"c","status":"FILLED","executedQty":"bad","avgPrice":"100","cumQuote":"100"}"#);
        assert_eq!(b.class,EconomicTruthClass::Unknown);
        let o=parse_okx_economic_truth_http(200,r#"{"code":"0","data":[{"ordId":"1","clOrdId":"c","state":"filled","accFillSz":"1","avgPx":"bad"}]}"#);
        assert_eq!(o.class,EconomicTruthClass::Unknown);
    }

    #[test]
    fn economic_truth_identity_mismatch_fails_closed() {
        let truth=parse_okx_economic_truth_http(200,r#"{"code":"0","data":[{"ordId":"o","clOrdId":"other","state":"filled","accFillSz":"1","avgPx":"100","fillTime":"1"}]}"#);
        let mut l=LegFillLedger::new("okx","TEST","buy","i","expected",SCALE_12).unwrap();
        assert_eq!(apply_order_economic_truth(&mut l,&truth),Err(EconomicTruthError::IdentityMismatch));
    }

    #[test]
    fn okx_fill_events_use_trade_id_and_normalize_fee_sign() {
        let body=r#"{"code":"0","data":[{"tradeId":"t1","ordId":"o1","fillPx":"100","fillSz":"4","fee":"-0.04","feeCcy":"USDT","fillTime":"10"},{"tradeId":"t2","ordId":"o1","fillPx":"101","fillSz":"6","fee":"0.01","feeCcy":"USDT","fillTime":"11"},{"tradeId":"other","ordId":"o2","fillPx":"1","fillSz":"1","fee":"-1","feeCcy":"USDT","fillTime":"12"}]}"#;
        let f=parse_okx_fill_events_http(200,body,"o1","TEST","USDT").unwrap();
        assert_eq!(f.len(),2);assert_eq!(f[0].fill_id,"t1");
        assert_eq!(f[0].fee_cost_quote_atoms,Some(econ_decimal_to_atoms("0.04").unwrap() as i128));
        assert_eq!(f[1].fee_cost_quote_atoms,Some(-(econ_decimal_to_atoms("0.01").unwrap() as i128)));
    }

    #[test]
    fn okx_base_currency_fee_is_converted_by_actual_fill_price() {
        let body=r#"{"code":"0","data":[{"tradeId":"t1","ordId":"o1","fillPx":"100","fillSz":"1","fee":"-0.001","feeCcy":"TEST","fillTime":"10"}]}"#;
        let f=parse_okx_fill_events_http(200,body,"o1","TEST","USDT").unwrap();
        assert_eq!(f[0].fee_cost_quote_atoms,Some(econ_decimal_to_atoms("0.1").unwrap() as i128));
    }

    #[test]
    fn binance_fill_events_use_trade_id_and_quote_commission() {
        let body=r#"[{"symbol":"TESTUSDT","id":7,"orderId":22,"price":"100.4","qty":"3","quoteQty":"301.2","commission":"0.12","commissionAsset":"USDT","time":10},{"symbol":"TESTUSDT","id":8,"orderId":22,"price":"100.5","qty":"7","quoteQty":"703.5","commission":"0.01","commissionAsset":"BNB","time":11}]"#;
        let f=parse_binance_fill_events_http(200,body,"22","USDT").unwrap();
        assert_eq!(f.len(),2);assert_eq!(f[0].fill_id,"7");
        assert_eq!(f[0].fee_cost_quote_atoms,Some(econ_decimal_to_atoms("0.12").unwrap() as i128));
        assert_eq!(f[1].fee_cost_quote_atoms,None);
    }

    #[test]
    fn read_only_fill_history_builders_are_signed_not_mutations() {
        let o=build_okx_fills_history_request(&okx_creds(),1_700_000_000_123,OkxDomain::Global,"TEST/USDT","123").unwrap();
        assert_eq!(o.method,HttpMethod::Get);assert!(!o.is_mutation);
        assert_eq!(o.url,"https://www.okx.com/api/v5/trade/fills-history?instType=SPOT&instId=TEST-USDT&ordId=123");
        assert_eq!(header(&o,"OK-ACCESS-SIGN"),Some("odx27G9RfUYLWbN3MjoNduyDl8TTQ16yCGbWGHtTJK0="));
        let b=build_binance_user_trades_request(&bn_creds(),1_700_000_000_123,10_000,"TEST/USDT:USDT","22").unwrap();
        assert_eq!(b.method,HttpMethod::Get);assert!(!b.is_mutation);
        assert_eq!(b.url,"https://fapi.binance.com/fapi/v1/userTrades?timestamp=1700000000123&symbol=TESTUSDT&orderId=22&recvWindow=10000&signature=12c8bad471ba2254f1400626ce8a511f79baa576be12fb481f0fd50aa54ffb30");
    }

    #[test]
    fn binance_cumquote_mismatch_fails_closed() {
        let truth=parse_binance_economic_truth_http(200,r#"{"orderId":22,"clientOrderId":"c2","status":"FILLED","executedQty":"10","avgPrice":"100","cumQuote":"999","updateTime":1}"#);
        let mut l=LegFillLedger::new("binance","TEST","sell","i","c2",10*SCALE_12).unwrap();
        assert_eq!(apply_order_economic_truth(&mut l,&truth),Err(EconomicTruthError::Economic(EconError::AverageMismatch)));
    }

    #[test]
    fn individual_fills_cross_check_reported_average() {
        let body=r#"{"code":"0","data":[{"ordId":"o1","clOrdId":"c1","state":"filled","accFillSz":"10","avgPx":"100.6","fillTime":"11"}]}"#;
        let truth=parse_okx_economic_truth_http(200,body);
        let mut l=LegFillLedger::new("okx","TEST","buy","i","c1",10*SCALE_12).unwrap();
        apply_order_economic_truth(&mut l,&truth).unwrap();
        let fills=parse_okx_fill_events_http(200,r#"{"code":"0","data":[{"tradeId":"1","ordId":"o1","fillPx":"100","fillSz":"4","fee":"-0.01","feeCcy":"USDT","fillTime":"10"},{"tradeId":"2","ordId":"o1","fillPx":"101","fillSz":"6","fee":"-0.01","feeCcy":"USDT","fillTime":"11"}]}"#,"o1","TEST","USDT").unwrap();
        for f in fills{l.apply_fill(f,DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();}
        assert_eq!(atoms_to_string(l.vwap_atoms().unwrap()),"100.6");
    }
}
