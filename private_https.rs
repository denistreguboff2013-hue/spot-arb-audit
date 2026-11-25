//! R17 native HTTPS/time/account-mode transport gate.
//!
//! Production HTTPS uses OS trust through native-tls with TLS >= 1.2,
//! exact host/path allowlists, bounded reads and finite timeouts.
//! No automatic retry exists in this layer.

use crate::execution_adapter::Venue;
use crate::authoritative_state::{AuthorizedCloseOrder,EntryNetworkPermit,NativeAuthoritativeState,NativeClosePermit,NativeEntryLeg,NativeStateError,NativeStateStore};
use crate::private_rest::{
    classify_transport_failure, parse_binance_order_http, parse_binance_position_mode_http,
    parse_okx_order_http, BinancePositionMode, HttpMethod, OkxDomain, OrderObservation,
    SignedRequest, PreparedEntryRequest,RestBuildError,CredentialsRef,
    build_signed_order_from_network_boundary,to_native_close_observation_e18,
    native_close_query_evidence_e18,build_binance_query_by_client_id,
    build_okx_query_by_client_id,
};
use native_tls::{Protocol, TlsConnector};
use serde_json::Value;
use std::{
    io::{self, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    sync::{atomic::{AtomicUsize, Ordering}, mpsc},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use url::Url;

pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 3_000;
pub const DEFAULT_IO_TIMEOUT_MS: u64 = 5_000;
pub const DEFAULT_TOTAL_TIMEOUT_MS: u64 = 12_000;
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 1_048_576;
pub const DEFAULT_MAX_HEADER_BYTES: usize = 32_768;
pub const DEFAULT_MAX_TIME_SAMPLE_RTT_MS: u64 = 5_000;
pub const DEFAULT_TIME_SAMPLE_MAX_AGE_MS: u64 = 30_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicTimeVenue {
    BinanceUsdm,
    Okx(OkxDomain),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpsRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
    pub credential_bearing: bool,
    pub credential_scope: Option<Venue>,
    pub mutation: bool,
}

impl HttpsRequest {
    pub fn public_time(venue: PublicTimeVenue) -> Self {
        match venue {
            PublicTimeVenue::BinanceUsdm => Self {
                method: HttpMethod::Get,
                url: "https://fapi.binance.com/fapi/v1/time".into(),
                headers: Vec::new(),
                body: None,
                credential_bearing: false,
                credential_scope: None,
                mutation: false,
            },
            PublicTimeVenue::Okx(domain) => Self {
                method: HttpMethod::Get,
                url: format!("{}/api/v5/public/time", domain.base_url()),
                headers: Vec::new(),
                body: None,
                credential_bearing: false,
                credential_scope: None,
                mutation: false,
            },
        }
    }

    pub fn from_signed(value: &SignedRequest) -> Self {
        let credential_bearing = value.headers.iter().any(|(name, _)| {
            name.eq_ignore_ascii_case("X-MBX-APIKEY")
                || name.eq_ignore_ascii_case("OK-ACCESS-KEY")
        });
        Self {
            method: value.method,
            url: value.url.clone(),
            headers: value.headers.clone(),
            body: value.body.clone(),
            credential_bearing,
            credential_scope: if credential_bearing { Some(value.venue) } else { None },
            mutation: value.is_mutation,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportErrorKind {
    InvalidUrl,
    EndpointDenied,
    InvalidHeader,
    Dns,
    ConnectTimeout,
    Connect,
    Tls,
    WriteTimeout,
    Write,
    ReadTimeout,
    Read,
    ResponseTooLarge,
    InvalidHttp,
    UnsupportedEncoding,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportError {
    pub kind: TransportErrorKind,
    pub detail: &'static str,
}

impl TransportError {
    fn new(kind: TransportErrorKind, detail: &'static str) -> Self {
        Self { kind, detail }
    }

    pub fn outcome_unknown(&self) -> bool {
        matches!(
            self.kind,
            TransportErrorKind::ConnectTimeout
                | TransportErrorKind::Connect
                | TransportErrorKind::Tls
                | TransportErrorKind::WriteTimeout
                | TransportErrorKind::Write
                | TransportErrorKind::ReadTimeout
                | TransportErrorKind::Read
                | TransportErrorKind::ResponseTooLarge
                | TransportErrorKind::InvalidHttp
                | TransportErrorKind::UnsupportedEncoding
        )
    }
}

#[derive(Clone, Debug)]
pub struct HttpsClient {
    connect_timeout: Duration,
    io_timeout: Duration,
    total_timeout: Duration,
    max_response_bytes: usize,
    max_header_bytes: usize,
}

impl Default for HttpsClient {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS),
            io_timeout: Duration::from_millis(DEFAULT_IO_TIMEOUT_MS),
            total_timeout: Duration::from_millis(DEFAULT_TOTAL_TIMEOUT_MS),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_header_bytes: DEFAULT_MAX_HEADER_BYTES,
        }
    }
}

// DNS and TLS handshakes may block beyond socket timeouts. Bound the
// *caller* by the same monotonic deadline and cap lingering workers.
// Only pre-HTTP work runs on a worker: no GET bytes can be transmitted
// by a worker after the caller has timed out. This is not hard cancellation.
static DNS_WORKERS:AtomicUsize=AtomicUsize::new(0);
static TLS_WORKERS:AtomicUsize=AtomicUsize::new(0);
const MAX_PRE_REQUEST_WORKERS:usize=4;

fn bounded_pre_request_stage<T:Send+'static>(
 deadline:Instant, step:Duration, kind:TransportErrorKind,
 detail:&'static str, active:&'static AtomicUsize,
 work:impl FnOnce()->Result<T,TransportError>+Send+'static,
)->Result<T,TransportError>{
 let budget=remaining_budget(deadline,step,kind,detail)?;
 active.fetch_update(Ordering::AcqRel,Ordering::Acquire,|n|
    (n<MAX_PRE_REQUEST_WORKERS).then_some(n+1))
    .map_err(|_|TransportError::new(kind,"pre-request worker capacity exhausted"))?;
 let (tx,rx)=mpsc::sync_channel(1);
 let spawned=thread::Builder::new().name("native-https-pre-request".into())
    .spawn(move||{
      struct Release(&'static AtomicUsize);
      impl Drop for Release{fn drop(&mut self){self.0.fetch_sub(1,Ordering::AcqRel);}}
      let _release=Release(active);
      let _=tx.send(work());
    });
 if spawned.is_err(){
    active.fetch_sub(1,Ordering::AcqRel);
    return Err(TransportError::new(kind,"cannot spawn pre-request worker"));
 }
 let result=rx.recv_timeout(budget)
    .map_err(|_|TransportError::new(kind,detail))?;
 remaining_budget(deadline,step,kind,detail)?;
 result
}

fn address_allowed(request:&HttpsRequest,address:&std::net::SocketAddr)->bool{
    !request.credential_bearing || address.is_ipv4()
}

impl HttpsClient {
    pub fn new(
        connect_timeout: Duration,
        io_timeout: Duration,
        max_response_bytes: usize,
    ) -> Result<Self, TransportError> {
        if connect_timeout.is_zero()
            || io_timeout.is_zero()
            || max_response_bytes == 0
            || max_response_bytes > 8 * 1024 * 1024
        {
            return Err(TransportError::new(
                TransportErrorKind::EndpointDenied,
                "invalid transport limits",
            ));
        }
        Ok(Self {
            connect_timeout,
            io_timeout,
            total_timeout: connect_timeout.saturating_add(io_timeout.saturating_mul(2))
                .min(Duration::from_secs(30)),
            max_response_bytes,
            max_header_bytes: DEFAULT_MAX_HEADER_BYTES,
        })
    }

    pub fn execute(&self, request: &HttpsRequest) -> Result<HttpResponse, TransportError> {
        // R32C: generic network transport is permanently read-only in review.
        // Mutation capability is NOT available through public HttpsClient or
        // the HttpExecutor trait implementation. This prevents bypassing the
        // native durable-intent boundary with caller-created POST requests.
        // A future production-native sender needs a separately certified,
        // privately held capability and explicit release authorization.
        if request.method != HttpMethod::Get || request.mutation {
            return Err(TransportError::new(
                TransportErrorKind::EndpointDenied,
                "native mutation transport capability unavailable",
            ));
        }
        let url = validate_request(request)?;
        // Reject malformed GETs before DNS or any network side effects.
        let wire = serialize_request(&url, request)?;
        let deadline = Instant::now() + self.total_timeout;
        let host = url
            .host_str()
            .ok_or_else(|| TransportError::new(TransportErrorKind::InvalidUrl, "missing host"))?;
        let port = url.port_or_known_default().ok_or_else(|| {
            TransportError::new(TransportErrorKind::InvalidUrl, "missing https port")
        })?;
        let mut last_connect_error = None;
        let lookup_host=host.to_owned();
        let addresses=bounded_pre_request_stage(
            deadline,self.connect_timeout,TransportErrorKind::Dns,
            "total DNS deadline",&DNS_WORKERS,move||{
                (lookup_host.as_str(),port).to_socket_addrs()
                    .map(|it|it.collect::<Vec<_>>())
                    .map_err(|_|TransportError::new(TransportErrorKind::Dns,"dns resolution"))
            })?;
        let mut tcp = None;
        for address in addresses {
            if !address_allowed(request,&address) {
                continue;
            }
            let budget = remaining_budget(deadline, self.connect_timeout,
                TransportErrorKind::ConnectTimeout, "total connect deadline")?;
            match TcpStream::connect_timeout(&address, budget) {
                Ok(stream) => {
                    tcp = Some(stream);
                    break;
                }
                Err(error) => last_connect_error = Some(error),
            }
        }
        let tcp = tcp.ok_or_else(|| {
            let timeout = last_connect_error
                .as_ref()
                .is_some_and(|e| e.kind() == io::ErrorKind::TimedOut);
            TransportError::new(
                if timeout {
                    TransportErrorKind::ConnectTimeout
                } else {
                    TransportErrorKind::Connect
                },
                "tcp connect",
            )
        })?;
        let tls_budget = remaining_budget(deadline, self.io_timeout,
            TransportErrorKind::Tls, "total tls deadline")?;
        tcp.set_read_timeout(Some(tls_budget))
            .map_err(|_| TransportError::new(TransportErrorKind::Read, "set read timeout"))?;
        tcp.set_write_timeout(Some(tls_budget))
            .map_err(|_| TransportError::new(TransportErrorKind::Write, "set write timeout"))?;

        let mut tls_builder = TlsConnector::builder();
        tls_builder.min_protocol_version(Some(Protocol::Tlsv12));
        let connector = tls_builder
            .build()
            .map_err(|_| TransportError::new(TransportErrorKind::Tls, "tls configuration"))?;
        let tls_host=host.to_owned();
        let mut stream=bounded_pre_request_stage(
            deadline,self.io_timeout,TransportErrorKind::Tls,
            "total TLS handshake deadline",&TLS_WORKERS,move||{
                connector.connect(&tls_host,tcp).map_err(|_|
                   TransportError::new(TransportErrorKind::Tls,"tls handshake"))
            })?;

        remaining_budget(deadline, self.io_timeout,
            TransportErrorKind::Tls, "total tls deadline")?;
        write_bounded_until(
            &mut stream,wire.as_bytes(),deadline,self.io_timeout,
            |tls,remaining|tls.get_ref().set_write_timeout(Some(remaining)),
        )?;

        let raw = read_bounded_until(
            &mut stream,
            self.max_header_bytes + self.max_response_bytes + 65_536,
            deadline, self.io_timeout,
            |tls, timeout| tls.get_ref().set_read_timeout(Some(timeout)),
        )?;
        let response = parse_http_response(
            &raw,
            self.max_header_bytes,
            self.max_response_bytes,
        )?;
        remaining_budget(deadline, self.io_timeout,
            TransportErrorKind::ReadTimeout, "total response deadline")?;
        Ok(response)
    }
}

pub trait HttpExecutor {
    fn execute_request(&self, request: &HttpsRequest) -> Result<HttpResponse, TransportError>;
}

impl HttpExecutor for HttpsClient {
    fn execute_request(&self, request: &HttpsRequest) -> Result<HttpResponse, TransportError> {
        self.execute(request)
    }
}

#[derive(Clone, Debug)]
pub struct ReadOnlyHttpsClient { inner: HttpsClient }
impl Default for ReadOnlyHttpsClient { fn default()->Self{Self{inner:HttpsClient::default()}} }
impl ReadOnlyHttpsClient {
    pub fn new(inner:HttpsClient)->Self{Self{inner}}
    pub fn execute(&self,request:&HttpsRequest)->Result<HttpResponse,TransportError>{
        if request.method!=HttpMethod::Get||request.mutation {
            return Err(TransportError::new(TransportErrorKind::EndpointDenied,"read-only transport gate"));
        }
        self.inner.execute(request)
    }
}
impl HttpExecutor for ReadOnlyHttpsClient {
    fn execute_request(&self,request:&HttpsRequest)->Result<HttpResponse,TransportError>{self.execute(request)}
}

/// TRADE2F3 Stage B2: examine the EXISTING GET policy without DNS, TLS,
/// socket creation, credentials use or execution. This is only a policy
/// inspection helper; a successful result is NEVER account-read approval.
/// In particular no caller-provided POST/mutation/body can be probed as GET.
pub fn inspect_readonly_get_policy_without_network(
    request:&HttpsRequest,
)->Result<(),TransportError>{
    if request.method!=HttpMethod::Get||request.mutation||request.body.is_some(){
        return Err(TransportError::new(TransportErrorKind::EndpointDenied,
                 "offline GET-only policy probe rejects mutation or body"));
    }
    validate_request(request).map(|_|())
}

/// Never trust caller-provided credential_bearing or credential_scope flags.
/// A raw HttpsRequest is public; its headers must independently agree with
/// the intended private venue before DNS, TLS or serialization occurs.
fn validate_header_credential_scope(request:&HttpsRequest)->Result<(),TransportError>{
    let deny=||TransportError::new(TransportErrorKind::InvalidHeader,
        "untrusted or conflicting credential headers");
    if request.headers.len()>32{return Err(deny())}
    let mut seen=std::collections::BTreeSet::new();
    let mut binance_key=0usize;
    let mut okx_key=0usize;
    let mut okx_signature=0usize;
    let mut okx_passphrase=0usize;
    let mut okx_timestamp=0usize;
    for (name,value) in &request.headers{
        if name.is_empty()||name.len()>128||value.is_empty()||value.len()>8192
           ||!name.bytes().all(|c|c.is_ascii_alphanumeric()
               ||b"!#$%&'*+-.^_`|~".contains(&c))
           ||!value.bytes().all(|c|(32..=126).contains(&c)){
            return Err(deny())
        }
        let lower=name.to_ascii_lowercase();
        if !seen.insert(lower.clone()){return Err(deny())}
        if matches!(lower.as_str(),
             "host"|"connection"|"content-length"|"transfer-encoding"
             |"authorization"|"proxy-authorization"|"cookie"|"set-cookie"
             |"expect"|"upgrade"|"trailer"|"te"|"accept-encoding"
             |"user-agent"|"accept"){
            return Err(deny())
        }
        // Signed builders need only five venue headers and Content-Type.
        // An unknown X-API-KEY / X-AUTH-TOKEN must not be relayed to a
        // different public endpoint under a caller's false scope flags.
        match lower.as_str(){
            "x-mbx-apikey"=>{binance_key+=1;},
            "ok-access-key"=>{okx_key+=1;},
            "ok-access-sign"=>{
                // SHA-256 HMAC encodes to exactly 44 base64 characters
                // with one '=' padding. Shape is NOT proof of a valid MAC.
                if value.len()!=44||!value.ends_with('=')
                  ||!value[..43].bytes().all(|c|
                    c.is_ascii_alphanumeric()||c==b'+'||c==b'/'){
                    return Err(deny())
                }
                okx_signature+=1;
            },
            "ok-access-passphrase"=>{okx_passphrase+=1;},
            "ok-access-timestamp"=>{
                let stamp=value.as_bytes();
                if stamp.len()!=24||!stamp.iter().enumerate().all(|(i,c)|
                  match i {
                    4|7=>*c==b'-',10=>*c==b'T',13|16=>*c==b':',
                    19=>*c==b'.',23=>*c==b'Z',
                    _=>c.is_ascii_digit(),
                  }){return Err(deny())}
                okx_timestamp+=1;
            },
            "content-type"=>{},
            _=>return Err(deny()),
        }
        if lower.starts_with("ok-access-")
            &&(!request.credential_bearing||request.credential_scope!=Some(Venue::OkxSpot)){
            return Err(deny())
        }
        if lower=="x-mbx-apikey"
            &&(!request.credential_bearing||request.credential_scope!=Some(Venue::BinanceUsdm)){
            return Err(deny())
        }
    }
    if request.credential_bearing{
        match request.credential_scope{
            Some(Venue::OkxSpot) if okx_key==1&&binance_key==0
                &&okx_signature==1&&okx_passphrase==1&&okx_timestamp==1=>{},
            Some(Venue::BinanceUsdm) if binance_key==1&&okx_key==0
                &&okx_signature==0&&okx_passphrase==0&&okx_timestamp==0=>{},
            _=>return Err(deny()),
        }
    }else if binance_key!=0||okx_key!=0||request.credential_scope.is_some(){
        return Err(deny())
    }
    Ok(())
}
fn validate_request(request: &HttpsRequest) -> Result<Url, TransportError> {
    // Both the offline probe and live executor must reject GET bodies, even
    // Some(""). A caller must not be able to change request framing.
    if request.method == HttpMethod::Get && request.body.is_some() {
        return Err(TransportError::new(TransportErrorKind::EndpointDenied,
            "GET body forbidden"));
    }
    // Avoid unbounded URL parsing and request serialization of private
    // query strings before any DNS/TLS work begins.
    if request.url.len() > 8192 {
        return Err(TransportError::new(TransportErrorKind::InvalidUrl,
            "https URL exceeds review transport limit"));
    }
    let url = Url::parse(&request.url)
        .map_err(|_| TransportError::new(TransportErrorKind::InvalidUrl, "url parse"))?;
    if url.scheme() != "https"
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
        || url.port_or_known_default() != Some(443)
    {
        return Err(TransportError::new(
            TransportErrorKind::EndpointDenied,
            "https endpoint shape",
        ));
    }
    validate_header_credential_scope(request)?;
    let host = url.host_str().unwrap_or("");
    if request.credential_bearing {
        match request.credential_scope {
            Some(Venue::BinanceUsdm) if host == "fapi.binance.com" => {}
            Some(Venue::OkxSpot)
                if ["www.okx.com", "eea.okx.com", "us.okx.com", "openapi.okx.com"]
                    .contains(&host) => {}
            _ => {
                return Err(TransportError::new(
                    TransportErrorKind::EndpointDenied,
                    "credential venue scope",
                ))
            }
        }
    } else if request.credential_scope.is_some() {
        return Err(TransportError::new(
            TransportErrorKind::EndpointDenied,
            "credential scope without credentials",
        ));
    }
    let path = url.path();
    let allowed = if host == "fapi.binance.com" {
        match (request.method, path, request.credential_bearing, request.mutation) {
            (HttpMethod::Get, "/fapi/v1/time", false, false) => url.query().is_none(),
            (HttpMethod::Get, "/fapi/v1/exchangeInfo", false, false) => url.query().is_none(),
            (HttpMethod::Get, "/fapi/v1/positionSide/dual", true, false) => true,
            (HttpMethod::Get, "/fapi/v1/accountConfig", true, false) => true,
            (HttpMethod::Get, "/fapi/v3/balance", true, false) => true,
            (HttpMethod::Get, "/fapi/v3/positionRisk", true, false) => true,
            (HttpMethod::Get, "/fapi/v1/openOrders", true, false) => true,
            (HttpMethod::Get, "/fapi/v1/order", true, false) => true,
            (HttpMethod::Get, "/fapi/v1/userTrades", true, false) => true,
            (HttpMethod::Post, "/fapi/v1/order", true, true) => true,
            _ => false,
        }
    } else if ["www.okx.com", "eea.okx.com", "us.okx.com", "openapi.okx.com"]
        .contains(&host)
    {
        match (request.method, path, request.credential_bearing, request.mutation) {
            (HttpMethod::Get, "/api/v5/public/time", false, false) => url.query().is_none(),
            (HttpMethod::Get, "/api/v5/public/instruments", false, false) => url.query() == Some("instType=SPOT"),
            (HttpMethod::Get, "/api/v5/account/config", true, false) => true,
            (HttpMethod::Get, "/api/v5/account/balance", true, false) => true,
            (HttpMethod::Get, "/api/v5/account/positions", true, false) => true,
            (HttpMethod::Get, "/api/v5/trade/orders-pending", true, false) => url.query() == Some("instType=SPOT"),
            (HttpMethod::Get, "/api/v5/trade/order", true, false) => true,
            (HttpMethod::Get, "/api/v5/trade/fills-history", true, false) => true,
            (HttpMethod::Post, "/api/v5/trade/order", true, true) => true,
            _ => false,
        }
    } else {
        false
    };
    if !allowed {
        return Err(TransportError::new(
            TransportErrorKind::EndpointDenied,
            "endpoint not allowlisted",
        ));
    }
    // Binance's HMAC lives in the URL query, not the API-key header.
    // An API key alone cannot qualify a private GET for transport.
    // This checks canonical signature ENVELOPE only, not HMAC authenticity.
    if request.credential_bearing&&host=="fapi.binance.com"
       &&request.method==HttpMethod::Get {
        let deny=||TransportError::new(TransportErrorKind::InvalidHeader,
            "private Binance GET missing or ambiguous signed query");
        let query=url.query().ok_or_else(deny)?;
        if query.len()>4096{return Err(deny())}
        let mut names=std::collections::BTreeSet::new();
        let mut signature=false;
        let mut timestamp=false;
        let mut recv_window=false;
        for (name,value) in url.query_pairs(){
            if !names.insert(name.to_string())||name.is_empty()
              ||name.len()>64||value.is_empty()||value.len()>256 {
                return Err(deny())
            }
            match name.as_ref(){
                "signature" => signature=value.len()==64&&value.bytes().all(|c|
                    c.is_ascii_digit()||(b'a'..=b'f').contains(&c)),
                "timestamp" => timestamp=value.parse::<u64>().ok()
                    .is_some_and(|n|n>0&&n.to_string()==value),
                "recvWindow" => recv_window=value.parse::<u64>().ok()
                    .is_some_and(|n|(1..=60_000).contains(&n)&&n.to_string()==value),
                _=>{},
            }
        }
        if !signature||!timestamp||!recv_window{return Err(deny())}
    }
    Ok(url)
}

fn serialize_request(url: &Url, request: &HttpsRequest) -> Result<String, TransportError> {
    let host = url.host_str().unwrap();
    let target = match url.query() {
        Some(query) => format!("{}?{}", url.path(), query),
        None => url.path().to_string(),
    };
    let method = match request.method {
        HttpMethod::Get => "GET",
        HttpMethod::Post => "POST",
    };
    let body = request.body.as_deref().unwrap_or("");
    if request.method == HttpMethod::Get && !body.is_empty() {
        return Err(TransportError::new(
            TransportErrorKind::EndpointDenied,
            "GET body forbidden",
        ));
    }
    let mut out = format!(
        "{method} {target} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: spotarb-native-core-r17\r\nAccept: application/json\r\nAccept-Encoding: identity\r\nConnection: close\r\n"
    );
    for (name, value) in &request.headers {
        out.push_str(name);
        out.push_str(": ");
        out.push_str(value);
        out.push_str("\r\n");
    }
    if request.method == HttpMethod::Post {
        out.push_str(&format!("Content-Length: {}\r\n", body.as_bytes().len()));
    }
    out.push_str("\r\n");
    out.push_str(body);
    Ok(out)
}

fn map_write_error(error: io::Error) -> TransportError {
    TransportError::new(
        if matches!(
            error.kind(),
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
        ) {
            TransportErrorKind::WriteTimeout
        } else {
            TransportErrorKind::Write
        },
        "https write",
    )
}

fn map_read_error(error: io::Error) -> TransportError {
    TransportError::new(
        if matches!(
            error.kind(),
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
        ) {
            TransportErrorKind::ReadTimeout
        } else {
            TransportErrorKind::Read
        },
        "https read",
    )
}

// The deadline is monotonic: every read is limited to the remaining
// total request budget, instead of resetting an IO timeout on each chunk.
fn remaining_budget(
    deadline: Instant, per_step: Duration, kind: TransportErrorKind,
    detail: &'static str,
) -> Result<Duration, TransportError> {
    let remaining = deadline.checked_duration_since(Instant::now())
        .ok_or_else(|| TransportError::new(kind, detail))?;
    if remaining.is_zero() {
        return Err(TransportError::new(kind, detail));
    }
    Ok(remaining.min(per_step))
}

// Monotonic total deadline: refresh socket budget for each TLS write and flush.
fn write_bounded_until<W: Write>(
    writer: &mut W, wire: &[u8], deadline: Instant, io_timeout: Duration,
    mut set_timeout: impl FnMut(&mut W, Duration) -> io::Result<()>,
) -> Result<(), TransportError> {
    let mut sent=0usize;
    while sent<wire.len() {
        let budget=remaining_budget(deadline,io_timeout,
            TransportErrorKind::WriteTimeout,"total write deadline")?;
        set_timeout(writer,budget).map_err(|_|
            TransportError::new(TransportErrorKind::Write,"set write timeout"))?;
        let n=writer.write(&wire[sent..]).map_err(map_write_error)?;
        remaining_budget(deadline,io_timeout,
            TransportErrorKind::WriteTimeout,"total write deadline")?;
        if n==0 {return Err(TransportError::new(
            TransportErrorKind::Write,"zero-byte write"))}
        sent+=n;
    }
    let budget=remaining_budget(deadline,io_timeout,
        TransportErrorKind::WriteTimeout,"total flush deadline")?;
    set_timeout(writer,budget).map_err(|_|
        TransportError::new(TransportErrorKind::Write,"set flush timeout"))?;
    writer.flush().map_err(map_write_error)?;
    remaining_budget(deadline,io_timeout,
        TransportErrorKind::WriteTimeout,"total flush deadline")?;
    Ok(())
}

fn read_bounded_until<R: Read>(
    reader: &mut R, max_wire_bytes: usize, deadline: Instant,
    io_timeout: Duration, mut set_timeout: impl FnMut(&mut R, Duration) -> io::Result<()>,
) -> Result<Vec<u8>, TransportError> {
    let mut out = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let budget = remaining_budget(deadline, io_timeout,
            TransportErrorKind::ReadTimeout, "total read deadline")?;
        set_timeout(reader, budget)
            .map_err(|_| TransportError::new(TransportErrorKind::Read, "set remaining read budget"))?;
        let n = reader.read(&mut buf).map_err(map_read_error)?;
        remaining_budget(deadline, io_timeout,
            TransportErrorKind::ReadTimeout, "total read deadline")?;
        if n == 0 { break; }
        if out.len().saturating_add(n) > max_wire_bytes {
            return Err(TransportError::new(TransportErrorKind::ResponseTooLarge, "wire response bound"));
        }
        out.extend_from_slice(&buf[..n]);
    }
    Ok(out)
}

fn parse_http_response(
    raw: &[u8],
    max_header_bytes: usize,
    max_body_bytes: usize,
) -> Result<HttpResponse, TransportError> {
    let split = raw
        .windows(4)
        .position(|x| x == b"\r\n\r\n")
        .ok_or_else(|| TransportError::new(TransportErrorKind::InvalidHttp, "header terminator"))?;
    let header_end = split + 4;
    if header_end > max_header_bytes {
        return Err(TransportError::new(
            TransportErrorKind::ResponseTooLarge,
            "header bound",
        ));
    }
    let header_text = std::str::from_utf8(&raw[..split])
        .map_err(|_| TransportError::new(TransportErrorKind::InvalidHttp, "header utf8"))?;
    let mut lines = header_text.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| TransportError::new(TransportErrorKind::InvalidHttp, "status line"))?;
    let mut status_parts = status_line.split_whitespace();
    let version = status_parts.next().unwrap_or("");
    let status: u16 = status_parts
        .next()
        .ok_or_else(|| TransportError::new(TransportErrorKind::InvalidHttp, "status code"))?
        .parse()
        .map_err(|_| TransportError::new(TransportErrorKind::InvalidHttp, "status parse"))?;
    if !version.starts_with("HTTP/1.") || !(100..600).contains(&status) {
        return Err(TransportError::new(
            TransportErrorKind::InvalidHttp,
            "http status",
        ));
    }
    let mut headers = Vec::new();
    let mut content_length = None;
    let mut chunked = false;
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| TransportError::new(TransportErrorKind::InvalidHttp, "header field"))?;
        let name = name.trim().to_string();
        let value = value.trim().to_string();
        if name.eq_ignore_ascii_case("content-length") {
            let parsed: usize = value.parse().map_err(|_| {
                TransportError::new(TransportErrorKind::InvalidHttp, "content length")
            })?;
            content_length = Some(parsed);
        }
        if name.eq_ignore_ascii_case("transfer-encoding")
            && value.to_ascii_lowercase().contains("chunked")
        {
            chunked = true;
        }
        if name.eq_ignore_ascii_case("content-encoding")
            && !value.eq_ignore_ascii_case("identity")
        {
            return Err(TransportError::new(
                TransportErrorKind::UnsupportedEncoding,
                "compressed response denied",
            ));
        }
        headers.push((name, value));
    }
    if content_length.is_some_and(|n| n > max_body_bytes) {
        return Err(TransportError::new(
            TransportErrorKind::ResponseTooLarge,
            "content length bound",
        ));
    }
    let encoded_body = &raw[header_end..];
    let body = if chunked {
        decode_chunked(encoded_body, max_body_bytes)?
    } else {
        if encoded_body.len() > max_body_bytes {
            return Err(TransportError::new(
                TransportErrorKind::ResponseTooLarge,
                "body bound",
            ));
        }
        if let Some(expected) = content_length {
            if encoded_body.len() != expected {
                return Err(TransportError::new(
                    TransportErrorKind::InvalidHttp,
                    "content length mismatch",
                ));
            }
        }
        encoded_body.to_vec()
    };
    let body = String::from_utf8(body)
        .map_err(|_| TransportError::new(TransportErrorKind::InvalidHttp, "body utf8"))?;
    Ok(HttpResponse {
        status,
        headers,
        body,
    })
}

fn decode_chunked(raw: &[u8], max_body_bytes: usize) -> Result<Vec<u8>, TransportError> {
    let mut cursor = 0usize;
    let mut out = Vec::new();
    loop {
        let line_end = raw[cursor..]
            .windows(2)
            .position(|x| x == b"\r\n")
            .map(|p| cursor + p)
            .ok_or_else(|| TransportError::new(TransportErrorKind::InvalidHttp, "chunk size"))?;
        let size_line = std::str::from_utf8(&raw[cursor..line_end])
            .map_err(|_| TransportError::new(TransportErrorKind::InvalidHttp, "chunk utf8"))?;
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|_| TransportError::new(TransportErrorKind::InvalidHttp, "chunk parse"))?;
        cursor = line_end + 2;
        if size == 0 {
            return Ok(out);
        }
        if out.len().saturating_add(size) > max_body_bytes
            || cursor.saturating_add(size + 2) > raw.len()
        {
            return Err(TransportError::new(
                if out.len().saturating_add(size) > max_body_bytes {
                    TransportErrorKind::ResponseTooLarge
                } else {
                    TransportErrorKind::InvalidHttp
                },
                "chunk bound",
            ));
        }
        out.extend_from_slice(&raw[cursor..cursor + size]);
        cursor += size;
        if raw.get(cursor..cursor + 2) != Some(b"\r\n") {
            return Err(TransportError::new(
                TransportErrorKind::InvalidHttp,
                "chunk terminator",
            ));
        }
        cursor += 2;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeSample {
    pub local_before_ms: u64,
    pub local_after_ms: u64,
    pub server_ms: u64,
    pub rtt_ms: u64,
    pub offset_ms: i64,
}

impl TimeSample {
    pub fn corrected_ms(&self, local_ms: u64) -> Option<u64> {
        let corrected = local_ms as i128 + self.offset_ms as i128;
        if corrected < 0 || corrected > u64::MAX as i128 {
            None
        } else {
            Some(corrected as u64)
        }
    }

    pub fn usable_at(&self, local_ms: u64, max_age_ms: u64, max_rtt_ms: u64) -> bool {
        self.rtt_ms <= max_rtt_ms
            && local_ms >= self.local_after_ms
            && local_ms - self.local_after_ms <= max_age_ms
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeError {
    HttpStatus,
    Parse,
    ClockWentBackwards,
    RttTooHigh,
}

pub fn parse_server_time(
    venue: PublicTimeVenue,
    response: &HttpResponse,
    local_before_ms: u64,
    local_after_ms: u64,
    max_rtt_ms: u64,
) -> Result<TimeSample, TimeError> {
    if response.status != 200 {
        return Err(TimeError::HttpStatus);
    }
    if local_after_ms < local_before_ms {
        return Err(TimeError::ClockWentBackwards);
    }
    let rtt_ms = local_after_ms - local_before_ms;
    if rtt_ms > max_rtt_ms {
        return Err(TimeError::RttTooHigh);
    }
    let value: Value = serde_json::from_str(&response.body).map_err(|_| TimeError::Parse)?;
    let server_ms = match venue {
        PublicTimeVenue::BinanceUsdm => value
            .get("serverTime")
            .and_then(Value::as_u64)
            .ok_or(TimeError::Parse)?,
        PublicTimeVenue::Okx(_) => value
            .get("data")
            .and_then(Value::as_array)
            .and_then(|x| x.first())
            .and_then(|x| x.get("ts"))
            .and_then(Value::as_str)
            .and_then(|x| x.parse::<u64>().ok())
            .ok_or(TimeError::Parse)?,
    };
    let midpoint = local_before_ms + rtt_ms / 2;
    let offset = server_ms as i128 - midpoint as i128;
    if offset < i64::MIN as i128 || offset > i64::MAX as i128 {
        return Err(TimeError::Parse);
    }
    Ok(TimeSample {
        local_before_ms,
        local_after_ms,
        server_ms,
        rtt_ms,
        offset_ms: offset as i64,
    })
}

pub fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

pub fn fetch_public_time<E: HttpExecutor>(
    executor: &E,
    venue: PublicTimeVenue,
) -> Result<TimeSample, TransportOrTimeError> {
    let request = HttpsRequest::public_time(venue);
    let before = wall_ms();
    let response = executor
        .execute_request(&request)
        .map_err(TransportOrTimeError::Transport)?;
    let after = wall_ms();
    parse_server_time(
        venue,
        &response,
        before,
        after,
        DEFAULT_MAX_TIME_SAMPLE_RTT_MS,
    )
    .map_err(TransportOrTimeError::Time)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportOrTimeError {
    Transport(TransportError),
    Time(TimeError),
}

/// R31 review-only entry dispatch gate. An in-memory, non-cloneable
/// authorization is consumed; state and safety are revalidated immediately
/// before the single transport attempt. The store lock fences concurrent
/// native state transitions until the attempt is finished.
#[derive(Debug,PartialEq,Eq)]
pub enum NativeEntryDispatchError{
    State(NativeStateError),
    HardSafety(crate::native_hard_safety::NativeHardSafetyError),
}

pub fn dispatch_authorized_entry_once<E:HttpExecutor>(
    executor:&E,store:&NativeStateStore,prepared:PreparedEntryRequest,
    current_safety_generation:u64,
)->Result<(OrderObservation,EntryNetworkPermit),NativeEntryDispatchError>{
    crate::native_hard_safety::require_native_mutation_egress()
        .map_err(NativeEntryDispatchError::HardSafety)?;
    // The same non-cloneable permit must follow the network observation into
    // durable state. UNKNOWN is reconciled; never retried blindly.
    let observation=store.with_authorized_entry_send(
        &prepared.authorized,current_safety_generation,
        ||dispatch_signed_once(executor,&prepared.request),
    ).map_err(NativeEntryDispatchError::State)?;
    Ok((observation,prepared.authorized.into_permit()))
}

/// Review-only, no live launch: derive and sign the exact durable close
/// order WHILE holding the authoritative writer lock through one HTTP attempt.
/// The returned permit is consumed later by the atomic native observation.
#[derive(Debug,PartialEq,Eq)]
pub enum NativeCloseDispatchError{
    State(NativeStateError),
    Sign(RestBuildError),
    HardSafety(crate::native_hard_safety::NativeHardSafetyError),
}

#[allow(clippy::too_many_arguments)]
pub fn dispatch_authorized_close_once<E:HttpExecutor>(
    executor:&E,store:&NativeStateStore,authorized:AuthorizedCloseOrder,
    current_safety_generation:u64,credentials:CredentialsRef<'_>,unix_ms:u64,
    okx_domain:OkxDomain,binance_position_mode:BinancePositionMode,
    binance_recv_window_ms:u64,
)->Result<(OrderObservation,NativeClosePermit),NativeCloseDispatchError>{
    crate::native_hard_safety::require_native_mutation_egress()
        .map_err(NativeCloseDispatchError::HardSafety)?;
    let observed=store.with_authorized_close_send(
        &authorized,current_safety_generation,|plan|{
            let signed=build_signed_order_from_network_boundary(
                &plan.network_action,&plan.intent,plan.kind,plan.reduce_only,None,
                current_safety_generation,current_safety_generation,
                credentials,unix_ms,okx_domain,binance_position_mode,
                binance_recv_window_ms,
            ).map_err(NativeCloseDispatchError::Sign)?;
            Ok(dispatch_order_once(executor,&signed))
        }
    ).map_err(NativeCloseDispatchError::State)??;
    Ok((observed,authorized.into_permit()))
}

/// R32B review-only handoff: the first HTTP attempt is unique, then the
/// observation is durably classified in native state before returning a
/// successful result to the caller. If persistence fails after HTTP, the
/// previously fsynced PREPARED intent remains unresolved and must be
/// reconciled. No automatic network retry is attempted.
#[derive(Debug,PartialEq,Eq)]
pub enum NativeRecordDispatchError{
    EntryDispatch(NativeEntryDispatchError),
    CloseDispatch(NativeCloseDispatchError),
    State(NativeStateError),
}

pub struct NativeRecordedOutcome{
    pub observation:OrderObservation,
    pub durable_state:NativeAuthoritativeState,
}

/// A placement response is not independent exchange fill/economic truth.
/// Conservatively record UNKNOWN for both native entry legs; R32C needs
/// authenticated order/fill queries before promoting either leg.
pub fn dispatch_record_native_entry_once<E:HttpExecutor>(
    executor:&E,store:&NativeStateStore,prepared:PreparedEntryRequest,
    current_safety_generation:u64,
)->Result<NativeRecordedOutcome,NativeRecordDispatchError>{
    let (observation,permit)=dispatch_authorized_entry_once(
        executor,store,prepared,current_safety_generation,
    ).map_err(NativeRecordDispatchError::EntryDispatch)?;
    let durable_state=match permit.leg(){
        NativeEntryLeg::Spot=>store.record_spot_entry_observation(
            permit,0,None,false,false,true),
        NativeEntryLeg::Futures=>{
            // MICRO2: in a prospectively signed/settled first leg, persist
            // the untrusted futures HTTP hint together with exact original
            // prepared client ID. Otherwise preserve legacy UNKNOWN paths
            // used by historical isolated unit-test fixtures.
            let before=store.load().map_err(NativeRecordDispatchError::State)?
                .ok_or(NativeRecordDispatchError::State(NativeStateError::Generation))?;
            let verified_first=before.entry.as_ref().map_or(false,|e|
                e.spot_prehedge_evidence.is_some()
                &&e.spot_prehedge_bills_evidence.is_some());
            if verified_first{
                store.quarantine_futures_dispatch_observation(
                    permit.generation(),&observation,wall_ms()).map(|x|x.0)
            }else{
                store.record_futures_entry_observation(
                    permit,0,None,false,false,true)
            }
        },
    }.map_err(NativeRecordDispatchError::State)?;
    Ok(NativeRecordedOutcome{observation,durable_state})
}

#[allow(clippy::too_many_arguments)]
pub fn dispatch_record_native_close_once<E:HttpExecutor>(
    executor:&E,store:&NativeStateStore,authorized:AuthorizedCloseOrder,
    current_safety_generation:u64,credentials:CredentialsRef<'_>,unix_ms:u64,
    okx_domain:OkxDomain,binance_position_mode:BinancePositionMode,
    binance_recv_window_ms:u64,
)->Result<NativeRecordedOutcome,NativeRecordDispatchError>{
    let expected_client=authorized.plan().intent.client_id.clone();
    let requested_e18=authorized.plan().requested_units;
    let (observation,permit)=dispatch_authorized_close_once(
        executor,store,authorized,current_safety_generation,credentials,
        unix_ms,okx_domain,binance_position_mode,binance_recv_window_ms,
    ).map_err(NativeRecordDispatchError::CloseDispatch)?;
    // Never use R15's e12 observation directly: native state uses e18.
    // UNKNOWN never changes confirmed quantities and blocks blind retries.
    let native=to_native_close_observation_e18(
        &observation,&expected_client,requested_e18);
    let durable_state=store.record_native_close_observation(permit,native)
        .map_err(NativeRecordDispatchError::State)?;
    Ok(NativeRecordedOutcome{observation,durable_state})
}

/// R32C: exact native close reconciliation via a signed read-only order GET.
/// A single "not found", transport timeout, absent/mismatching client ID,
/// nonterminal result or ambiguous quantity never authorizes a blind resend.
#[derive(Debug,PartialEq,Eq)]
pub enum NativeQueryRecoveryError{
    State(NativeStateError),Build(RestBuildError),
    CredentialVenue,
}
pub struct NativeQueryRecoveryResult{
    pub observation:OrderObservation,
    pub evidence_applied:bool,
    pub state:NativeAuthoritativeState,
}
pub fn query_native_close_recovery_once<E:HttpExecutor>(
    executor:&E,store:&NativeStateStore,expected_generation:u64,
    credentials:CredentialsRef<'_>,unix_ms:u64,
    okx_domain:OkxDomain,binance_recv_window_ms:u64,
)->Result<NativeQueryRecoveryResult,NativeQueryRecoveryError>{
    let target=store.pending_native_close_query(expected_generation)
        .map_err(NativeQueryRecoveryError::State)?;
    let signed=match (target.venue,credentials){
        (Venue::BinanceUsdm,CredentialsRef::Binance(c))=>
            build_binance_query_by_client_id(c,unix_ms,binance_recv_window_ms,
                &target.symbol,&target.client_id),
        (Venue::OkxSpot,CredentialsRef::Okx(c))=>
            build_okx_query_by_client_id(c,unix_ms,okx_domain,
                &target.symbol,&target.client_id),
        _=>return Err(NativeQueryRecoveryError::CredentialVenue),
    }.map_err(NativeQueryRecoveryError::Build)?;
    if signed.method!=HttpMethod::Get||signed.is_mutation
        ||signed.client_order_id.as_deref()!=Some(target.client_id.as_str()){
        return Err(NativeQueryRecoveryError::CredentialVenue)
    }
    let observation=query_order_once(executor,&signed);
    let evidence=native_close_query_evidence_e18(
        &observation,&target.client_id,target.requested_atoms_e18);
    match evidence{
        Some(e)=>{
            let state=store.recover_native_close(target.generation,true,e)
                .map_err(NativeQueryRecoveryError::State)?;
            Ok(NativeQueryRecoveryResult{observation,evidence_applied:true,state})
        },
        None=>{
            // Prepared/Unknown remains on disk. Only the read-only recovery
            // view is returned; zero state writes, zero re-sends.
            let state=store.load_for_recovery()
                .map_err(NativeQueryRecoveryError::State)?
                .ok_or(NativeQueryRecoveryError::State(NativeStateError::Generation))?;
            if state.generation!=target.generation{
                return Err(NativeQueryRecoveryError::State(NativeStateError::Generation))
            }
            Ok(NativeQueryRecoveryResult{observation,evidence_applied:false,state})
        }
    }
}

/// The legacy generic path is strictly close-only.
pub fn dispatch_order_once<E:HttpExecutor>(
    executor:&E, request:&SignedRequest,
)->OrderObservation{
    if crate::native_hard_safety::require_native_mutation_egress().is_err(){
        return native_review_egress_denied(request)
    }
    if !legacy_exact_close(request){
        return OrderObservation{
            class:crate::private_rest::RestOutcomeClass::Unknown,
            terminal:false,filled:None,status:None,order_id:None,
            client_order_id:request.client_order_id.clone(),
            reason:"NATIVE_ENTRY_PERMIT_REQUIRED",
        }
    }
    dispatch_signed_once(executor,request)
}

fn legacy_exact_close(request:&SignedRequest)->bool{
    if request.method!=HttpMethod::Post || !request.is_mutation{return false}
    let Some(body)=request.body.as_deref()else{return false};
    match request.venue{
        Venue::OkxSpot=>{
            if !request.url.ends_with("/api/v5/trade/order"){return false}
            let Ok(v)=serde_json::from_str::<Value>(body)else{return false};
            v.get("side").and_then(Value::as_str)==Some("sell")
                && v.get("ordType").and_then(Value::as_str)==Some("market")
        }
        Venue::BinanceUsdm=>{
            if request.url!="https://fapi.binance.com/fapi/v1/order"{return false}
            let pairs:std::collections::HashMap<_,_>=url::form_urlencoded::parse(body.as_bytes()).into_owned().collect();
            pairs.get("side").map(String::as_str)==Some("BUY")
                && pairs.get("type").map(String::as_str)==Some("MARKET")
                && pairs.get("reduceOnly").map(String::as_str)==Some("true")
        }
    }
}

fn native_review_egress_denied(request:&SignedRequest)->OrderObservation{
    OrderObservation{
        class:crate::private_rest::RestOutcomeClass::Unknown,
        terminal:false,filled:None,status:None,order_id:None,
        client_order_id:request.client_order_id.clone(),
        reason:"NATIVE_HARD_SAFETY_ENGAGED",
    }
}

fn dispatch_signed_once<E:HttpExecutor>(
    executor:&E,request:&SignedRequest
)->OrderObservation{
    if crate::native_hard_safety::require_native_mutation_egress().is_err(){
        return native_review_egress_denied(request)
    }
    let https = HttpsRequest::from_signed(request);
    match executor.execute_request(&https) {
        Ok(response) => match request.venue {
            Venue::BinanceUsdm => parse_binance_order_http(response.status, &response.body),
            Venue::OkxSpot => parse_okx_order_http(response.status, &response.body, false),
        },
        Err(_) => classify_transport_failure(),
    }
}

pub fn query_order_once<E: HttpExecutor>(
    executor: &E,
    request: &SignedRequest,
) -> OrderObservation {
    // R32D6C1: a "read-only" query helper must never become a disguised
    // POST egress with a caller-crafted SignedRequest and custom executor.
    if request.method!=HttpMethod::Get||request.is_mutation{
        return native_review_egress_denied(request)
    }
    let https = HttpsRequest::from_signed(request);
    match executor.execute_request(&https) {
        Ok(response) => match request.venue {
            Venue::BinanceUsdm => parse_binance_order_http(response.status, &response.body),
            Venue::OkxSpot => parse_okx_order_http(response.status, &response.body, true),
        },
        Err(_) => classify_transport_failure(),
    }
}

pub fn read_binance_position_mode_once<E: HttpExecutor>(
    executor: &E,
    request: &SignedRequest,
) -> BinancePositionMode {
    if request.method!=HttpMethod::Get||request.is_mutation{
        return BinancePositionMode::Unknown
    }
    let https = HttpsRequest::from_signed(request);
    match executor.execute_request(&https) {
        Ok(response) => parse_binance_position_mode_http(response.status, &response.body),
        Err(_) => BinancePositionMode::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p108_get_some_empty_body_denied_in_probe_and_executor() {
        let mut request = HttpsRequest::public_time(PublicTimeVenue::BinanceUsdm);
        assert!(validate_request(&request).is_ok());
        request.body = Some(String::new());
        assert_eq!(inspect_readonly_get_policy_without_network(&request)
            .unwrap_err().kind, TransportErrorKind::EndpointDenied);
        assert_eq!(HttpsClient::default().execute(&request)
            .unwrap_err().kind, TransportErrorKind::EndpointDenied);
    }

    #[test]
    fn p108_oversized_url_denied_before_network() {
        let mut request = HttpsRequest::public_time(PublicTimeVenue::Okx(OkxDomain::Global));
        request.url.push_str("?cursor=");
        request.url.push_str(&"a".repeat(8192));
        assert_eq!(inspect_readonly_get_policy_without_network(&request)
            .unwrap_err().kind, TransportErrorKind::InvalidUrl);
        assert_eq!(HttpsClient::default().execute(&request)
            .unwrap_err().kind, TransportErrorKind::InvalidUrl);
    }

    #[test]
    fn p108_dns_stage_times_out_before_blocking_resolver_finishes() {
        let start=Instant::now();
        let err=bounded_pre_request_stage(
            start+Duration::from_millis(18),Duration::from_millis(100),
            TransportErrorKind::Dns,"DNS bounded test",&DNS_WORKERS,
            ||{thread::sleep(Duration::from_millis(400));Ok::<_,TransportError>(())}
        ).unwrap_err();
        assert_eq!(err.kind,TransportErrorKind::Dns);
        assert!(start.elapsed()<Duration::from_millis(250));
    }

    #[test]
    fn p108_tls_stage_bounded_without_http_bytes_or_real_network() {
        let start=Instant::now();
        let err=bounded_pre_request_stage(
            start+Duration::from_millis(18),Duration::from_millis(100),
            TransportErrorKind::Tls,"TLS bounded test",&TLS_WORKERS,
            ||{thread::sleep(Duration::from_millis(400));Ok::<_,TransportError>(7u8)}
        ).unwrap_err();
        assert_eq!(err.kind,TransportErrorKind::Tls);
        assert!(start.elapsed()<Duration::from_millis(250));
        let ok=bounded_pre_request_stage(
            Instant::now()+Duration::from_secs(1),Duration::from_millis(200),
            TransportErrorKind::Tls,"TLS quick test",&TLS_WORKERS,
            ||Ok::<_,TransportError>(42u8)
        ).unwrap();
        assert_eq!(ok,42);
    }

    #[test]
    fn p108_pre_request_worker_cap_and_elapsed_deadline_fail_closed() {
        static FULL:AtomicUsize=AtomicUsize::new(MAX_PRE_REQUEST_WORKERS);
        let deny=bounded_pre_request_stage(
            Instant::now()+Duration::from_secs(1),Duration::from_millis(100),
            TransportErrorKind::Dns,"DNS worker capacity test",&FULL,
            ||Ok::<_,TransportError>(9u8)
        ).unwrap_err();
        assert_eq!(deny.kind,TransportErrorKind::Dns);
        assert_eq!(FULL.load(Ordering::Acquire),MAX_PRE_REQUEST_WORKERS);
        static UNSTARTED:AtomicUsize=AtomicUsize::new(0);
        let deny=bounded_pre_request_stage(
            Instant::now()-Duration::from_millis(1),Duration::from_millis(100),
            TransportErrorKind::Tls,"expired pre-request deadline",&UNSTARTED,
            ||Ok::<_,TransportError>(9u8)
        ).unwrap_err();
        assert_eq!(deny.kind,TransportErrorKind::Tls);
        assert_eq!(UNSTARTED.load(Ordering::Acquire),0);
    }

    #[test]
    fn p108_timed_out_workers_keep_capacity_until_exit_and_recover() {
        use std::sync::{Arc,Condvar,Mutex};
        static ACTIVE:AtomicUsize=AtomicUsize::new(0);
        let gate=Arc::new((Mutex::new(false),Condvar::new()));
        let (ready_tx,ready_rx)=mpsc::channel::<()>();
        let mut callers=Vec::new();
        for _ in 0..MAX_PRE_REQUEST_WORKERS {
            let release=Arc::clone(&gate);
            let ready=ready_tx.clone();
            callers.push(thread::spawn(move||{
                bounded_pre_request_stage(
                    Instant::now()+Duration::from_secs(8),Duration::from_secs(2),
                    TransportErrorKind::Dns,"synthetic timed-out resolver",&ACTIVE,
                    move||{
                        ready.send(()).unwrap();
                        let (lock,cv)=&*release;
                        let mut done=lock.lock().unwrap();
                        while !*done { done=cv.wait(done).unwrap(); }
                        Ok::<_,TransportError>(())
                    })
            }));
        }
        for _ in 0..MAX_PRE_REQUEST_WORKERS {
            ready_rx.recv_timeout(Duration::from_secs(4)).unwrap();
        }
        // The callers time out while resolver workers deliberately remain busy.
        for caller in callers {
            assert_eq!(caller.join().unwrap().unwrap_err().kind,TransportErrorKind::Dns);
        }
        assert_eq!(ACTIVE.load(Ordering::Acquire),MAX_PRE_REQUEST_WORKERS);
        let deny=bounded_pre_request_stage(
            Instant::now()+Duration::from_secs(1),Duration::from_secs(1),
            TransportErrorKind::Dns,"synthetic capacity exhausted",&ACTIVE,
            ||Ok::<_,TransportError>(())
        ).unwrap_err();
        assert_eq!(deny.kind,TransportErrorKind::Dns);
        assert_eq!(ACTIVE.load(Ordering::Acquire),MAX_PRE_REQUEST_WORKERS);
        {
            let (lock,cv)=&*gate;
            *lock.lock().unwrap()=true;
            cv.notify_all();
        }
        let cleanup_deadline=Instant::now()+Duration::from_secs(3);
        while ACTIVE.load(Ordering::Acquire)!=0 && Instant::now()<cleanup_deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(ACTIVE.load(Ordering::Acquire),0);
        bounded_pre_request_stage(
            Instant::now()+Duration::from_secs(1),Duration::from_secs(1),
            TransportErrorKind::Dns,"synthetic recovered resolver",&ACTIVE,
            ||Ok::<_,TransportError>(())
        ).unwrap();
        // recv_timeout observes the worker's send before its Drop guard
        // necessarily decrements ACTIVE. Wait boundedly for actual exit.
        let settled_by=Instant::now()+Duration::from_secs(2);
        while ACTIVE.load(Ordering::Acquire)!=0 && Instant::now()<settled_by{
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(ACTIVE.load(Ordering::Acquire),0);
    }

    #[test]
    fn p108_partial_tls_writes_share_one_absolute_deadline() {
        struct SlowWriter {written:usize}
        impl Write for SlowWriter {
            fn write(&mut self, _: &[u8])->io::Result<usize>{
                thread::sleep(Duration::from_millis(9));
                self.written+=1;Ok(1)
            }
            fn flush(&mut self)->io::Result<()>{Ok(())}
        }
        let mut w=SlowWriter{written:0};
        let deadline=Instant::now()+Duration::from_millis(22);
        let err=write_bounded_until(&mut w,&[7u8;30],deadline,
            Duration::from_millis(200),|_,_|Ok(())).unwrap_err();
        assert_eq!(err.kind,TransportErrorKind::WriteTimeout);
        assert!(w.written<30);
        let mut out=Vec::new();
        write_bounded_until(&mut out,b"safe-local",Instant::now()+Duration::from_secs(1),
            Duration::from_millis(200),|_,_|Ok(())).unwrap();
        assert_eq!(out,b"safe-local");
    }

    #[test]
    fn p108_slow_tls_flush_is_not_success_after_deadline(){
        struct SlowFlush;
        impl Write for SlowFlush {
            fn write(&mut self,bytes:&[u8])->io::Result<usize>{Ok(bytes.len())}
            fn flush(&mut self)->io::Result<()>{
                thread::sleep(Duration::from_millis(28));Ok(())
            }
        }
        let err=write_bounded_until(&mut SlowFlush,b"test",
            Instant::now()+Duration::from_millis(12),Duration::from_millis(200),
            |_,_|Ok(())).unwrap_err();
        assert_eq!(err.kind,TransportErrorKind::WriteTimeout);
    }

    #[test]
    fn p108_slow_drip_expires_total_monotonic_read_deadline() {
        struct Drip { bytes_left: usize }
        impl Read for Drip {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                std::thread::sleep(Duration::from_millis(9));
                if self.bytes_left == 0 { return Ok(0); }
                buf[0] = b'x';
                self.bytes_left -= 1;
                Ok(1)
            }
        }
        let mut drip = Drip { bytes_left: 50 };
        let deadline = Instant::now() + Duration::from_millis(22);
        let err = read_bounded_until(&mut drip, 1024, deadline,
            Duration::from_millis(200), |_reader, _budget| Ok(())).unwrap_err();
        assert_eq!(err.kind, TransportErrorKind::ReadTimeout);
        assert!(drip.bytes_left >= 47);
    }

    #[test]
    fn p108_monotonic_remaining_budget_shrinks_and_expiration_denies() {
        let deadline = Instant::now() + Duration::from_millis(30);
        let first = remaining_budget(deadline, Duration::from_millis(200),
            TransportErrorKind::ReadTimeout, "test").unwrap();
        std::thread::sleep(Duration::from_millis(3));
        let second = remaining_budget(deadline, Duration::from_millis(200),
            TransportErrorKind::ReadTimeout, "test").unwrap();
        assert!(second < first);
        assert_eq!(
            remaining_budget(Instant::now() - Duration::from_millis(1),
                Duration::from_millis(200), TransportErrorKind::ReadTimeout, "test")
                .unwrap_err().kind,
            TransportErrorKind::ReadTimeout
        );
    }

    #[test]
    fn p108_bad_get_body_is_denied_before_dns_or_tls() {
        let mut request = HttpsRequest::public_time(PublicTimeVenue::BinanceUsdm);
        request.body = Some("not permitted".into());
        assert_eq!(HttpsClient::default().execute(&request).unwrap_err().kind,
            TransportErrorKind::EndpointDenied);
    }

    use crate::execution::{Action as ExecAction, Leg};
    use crate::execution_adapter::{OrderIntent, OrderKind};
    use crate::private_rest::{
        build_binance_account_config_request, build_binance_balance_v3_request,
        build_binance_open_orders_request, build_binance_position_mode_request,
        build_binance_position_risk_v3_request, build_okx_account_config_request,
        build_okx_balance_request, build_okx_pending_spot_orders_request,
        build_okx_positions_request, build_signed_order_from_network_boundary,
        to_lifecycle_mutation_observation, BinanceCredentials, CredentialsRef, OkxCredentials,
        BINANCE_DEFAULT_RECV_WINDOW_MS,
    };
    use crate::execution::IntentState;
    use crate::execution_store::DurableMutationGate;
    use crate::position_lifecycle::{
        DurableLifecycleStore, LifecycleAction, LifecycleState, PositionLifecycle,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime as StdSystemTime, UNIX_EPOCH as StdUnixEpoch};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    #[derive(Clone)]
    struct FakeExecutor {
        result: Result<HttpResponse, TransportError>,
        calls: std::sync::Arc<AtomicUsize>,
        last: std::sync::Arc<Mutex<Option<HttpsRequest>>>,
    }

    impl FakeExecutor {
        fn response(status: u16, body: &str) -> Self {
            Self {
                result: Ok(HttpResponse {
                    status,
                    headers: Vec::new(),
                    body: body.into(),
                }),
                calls: Default::default(),
                last: Default::default(),
            }
        }

        fn error(kind: TransportErrorKind) -> Self {
            Self {
                result: Err(TransportError::new(kind, "fake")),
                calls: Default::default(),
                last: Default::default(),
            }
        }
    }

    impl HttpExecutor for FakeExecutor {
        fn execute_request(&self, request: &HttpsRequest) -> Result<HttpResponse, TransportError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.last.lock().unwrap() = Some(request.clone());
            self.result.clone()
        }
    }

    fn binance_creds() -> BinanceCredentials {
        BinanceCredentials::new("TEST_BIN_KEY", "TEST_BIN_SECRET").unwrap()
    }

    fn signed_binance_close() -> SignedRequest {
        let intent = OrderIntent {
            venue: Venue::BinanceUsdm,
            symbol: "BNT/USDT:USDT".into(),
            side: "buy".into(),
            amount: "10".into(),
            client_id: "sab00000000000000000000000000004".into(),
        };
        build_signed_order_from_network_boundary(
            &ExecAction::NetworkMutation {
                intent_id: "close-1".into(),
                leg: Leg::Futures,
            },
            &intent,
            OrderKind::Market,
            true,
            None,
            7,
            7,
            CredentialsRef::Binance(&binance_creds()),
            1_700_000_000_123,
            OkxDomain::Global,
            BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
        )
        .unwrap()
    }

    #[test]
    fn r32c_concrete_https_client_blocks_all_trade_post_before_dns(){
        let client=HttpsClient::default();
        let signed=signed_binance_close();
        let trade=HttpsRequest::from_signed(&signed);
        let denied=client.execute(&trade).unwrap_err();
        assert_eq!(denied.kind,TransportErrorKind::EndpointDenied);
        assert_eq!(denied.detail,"native mutation transport capability unavailable");
        assert_eq!(HttpExecutor::execute_request(&client,&trade)
            .unwrap_err().kind,TransportErrorKind::EndpointDenied);
        let mut spoof=trade;
        spoof.mutation=false;
        assert_eq!(client.execute(&spoof).unwrap_err().kind,
            TransportErrorKind::EndpointDenied);
        // Read-only GET has a separate code path. No actual query is sent.
        let public=HttpsRequest::public_time(PublicTimeVenue::BinanceUsdm);
        assert_eq!(public.method,HttpMethod::Get);
        assert!(!public.mutation);
        assert!(validate_request(&public).is_ok());
    }

    #[test]
    fn r31_unsigned_authority_cannot_use_generic_http_dispatch(){
        let fake=FakeExecutor::response(200,"{}");
        let mut future=signed_binance_close();
        future.body=Some(future.body.unwrap().replace("side=BUY","side=SELL")
            .replace("&reduceOnly=true",""));
        let denied=dispatch_order_once(&fake,&future);
        assert_eq!(denied.reason,"NATIVE_ENTRY_PERMIT_REQUIRED");
        assert_eq!(fake.calls.load(Ordering::SeqCst),0);
        let spot=SignedRequest{
            venue:Venue::OkxSpot,method:HttpMethod::Post,
            url:"https://www.okx.com/api/v5/trade/order".into(),
            headers:Vec::new(),
            body:Some(r#"{"side":"buy","ordType":"fok"}"#.into()),
            client_order_id:Some("fake".into()),is_mutation:true,
        };
        let denied=dispatch_order_once(&fake,&spot);
        assert_eq!(denied.reason,"NATIVE_ENTRY_PERMIT_REQUIRED");
        assert_eq!(fake.calls.load(Ordering::SeqCst),0);
    }

    #[test]
    fn strict_endpoint_allowlist_rejects_lookalikes_and_http() {
        for url in [
            "http://fapi.binance.com/fapi/v1/time",
            "https://fapi.binance.com.evil.example/fapi/v1/time",
            "https://user@fapi.binance.com/fapi/v1/time",
            "https://fapi.binance.com:444/fapi/v1/time",
            "https://fapi.binance.com/fapi/v1/order",
        ] {
            let req = HttpsRequest {
                method: HttpMethod::Get,
                url: url.into(),
                headers: Vec::new(),
                body: None,
                credential_bearing: false,
                credential_scope: None,
                mutation: false,
            };
            assert_eq!(
                validate_request(&req).unwrap_err().kind,
                TransportErrorKind::EndpointDenied
            );
        }
    }

    #[test]
    fn only_exact_private_paths_accept_credentials() {
        let mode = build_binance_position_mode_request(
            &binance_creds(),
            1_700_000_000_123,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
        )
        .unwrap();
        assert!(validate_request(&HttpsRequest::from_signed(&mode)).is_ok());
        let close = signed_binance_close();
        assert!(validate_request(&HttpsRequest::from_signed(&close)).is_ok());
    }

    #[test]
    fn r26_read_only_transport_rejects_mutation_before_network() {
        let client=ReadOnlyHttpsClient::default();
        let req=HttpsRequest{method:HttpMethod::Post,url:"https://fapi.binance.com/fapi/v1/order".into(),
            headers:Vec::new(),body:Some(String::new()),credential_bearing:false,credential_scope:None,mutation:true};
        let err=client.execute(&req).unwrap_err();
        assert_eq!(err.kind,TransportErrorKind::EndpointDenied);
        assert_eq!(err.detail,"read-only transport gate");
    }

    #[test]
    fn credential_bearing_transport_is_ipv4_only() {
        use std::net::{Ipv4Addr,Ipv6Addr,SocketAddr,SocketAddrV4,SocketAddrV6};
        let private=HttpsRequest{
            method:HttpMethod::Get,url:"https://www.okx.com/api/v5/account/config".into(),
            headers:vec![("OK-ACCESS-KEY".into(),"x".into())],body:None,
            credential_bearing:true,credential_scope:Some(Venue::OkxSpot),mutation:false,
        };
        let public=HttpsRequest::public_time(PublicTimeVenue::Okx(OkxDomain::Global));
        let v4=SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST,443));
        let v6=SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST,443,0,0));
        assert!(address_allowed(&private,&v4));
        assert!(!address_allowed(&private,&v6));
        assert!(address_allowed(&public,&v4));
        assert!(address_allowed(&public,&v6));
    }

    #[test]
    fn r26_read_only_private_truth_paths_are_allowlisted_and_non_mutating() {
        let bn=BinanceCredentials::new("TEST_BIN_KEY","TEST_BIN_SECRET").unwrap();
        let ok=OkxCredentials::new("TEST_OKX_KEY","TEST_OKX_SECRET","TEST_OKX_PASS").unwrap();
        let bin=[
            build_binance_account_config_request(&bn,1_700_000_000_123,10_000).unwrap(),
            build_binance_balance_v3_request(&bn,1_700_000_000_123,10_000).unwrap(),
            build_binance_position_risk_v3_request(&bn,1_700_000_000_123,10_000).unwrap(),
            build_binance_open_orders_request(&bn,1_700_000_000_123,10_000).unwrap(),
        ];
        let okx=[
            build_okx_account_config_request(&ok,1_700_000_000_123,OkxDomain::Global).unwrap(),
            build_okx_balance_request(&ok,1_700_000_000_123,OkxDomain::Global).unwrap(),
            build_okx_positions_request(&ok,1_700_000_000_123,OkxDomain::Global).unwrap(),
            build_okx_pending_spot_orders_request(&ok,1_700_000_000_123,OkxDomain::Global).unwrap(),
        ];
        for req in bin.iter().chain(okx.iter()) {
            let h=HttpsRequest::from_signed(req);assert!(!h.mutation);assert_eq!(h.method,HttpMethod::Get);
            assert!(validate_request(&h).is_ok(),"{:?}",req);
        }
        let mut denied=HttpsRequest::from_signed(&bin[0]);denied.method=HttpMethod::Post;denied.mutation=false;
        assert_eq!(validate_request(&denied).unwrap_err().kind,TransportErrorKind::EndpointDenied);
        let mut wrong=HttpsRequest::from_signed(&okx[3]);wrong.url="https://www.okx.com/api/v5/trade/orders-pending?instType=SWAP".into();
        assert_eq!(validate_request(&wrong).unwrap_err().kind,TransportErrorKind::EndpointDenied);
    }

    #[test]
    fn header_injection_and_transport_headers_are_denied() {
        for (name, value) in [
            ("X-Test\r\nInjected", "x"),
            ("X-Test", "x\r\nInjected: y"),
            ("Host", "evil.example"),
            ("Content-Length", "999"),
            ("Transfer-Encoding", "chunked"),
        ] {
            let req = HttpsRequest {
                method: HttpMethod::Get,
                url: "https://fapi.binance.com/fapi/v1/time".into(),
                headers: vec![(name.into(), value.into())],
                body: None,
                credential_bearing: false,
                credential_scope: None,
                mutation: false,
            };
            assert_eq!(
                validate_request(&req).unwrap_err().kind,
                TransportErrorKind::InvalidHeader
            );
        }
    }

    #[test]
    fn request_serialization_has_no_hidden_retry_or_redirect_surface() {
        let req = HttpsRequest::public_time(PublicTimeVenue::BinanceUsdm);
        let url = validate_request(&req).unwrap();
        let wire = serialize_request(&url, &req).unwrap();
        assert!(wire.starts_with("GET /fapi/v1/time HTTP/1.1\r\n"));
        assert!(wire.contains("Host: fapi.binance.com\r\n"));
        assert!(wire.contains("Accept-Encoding: identity\r\n"));
        assert!(wire.contains("Connection: close\r\n"));
    }

    #[test]
    fn parses_content_length_response() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 18\r\n\r\n{\"serverTime\":123}";
        let r = parse_http_response(raw, 1024, 1024).unwrap();
        assert_eq!(r.status, 200);
        assert_eq!(r.body, "{\"serverTime\":123}");
    }

    #[test]
    fn parses_chunked_response_with_extensions() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5;foo=bar\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let r = parse_http_response(raw, 1024, 1024).unwrap();
        assert_eq!(r.body, "hello world");
    }

    #[test]
    fn oversized_response_fails_closed() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n";
        assert_eq!(
            parse_http_response(raw, 1024, 10).unwrap_err().kind,
            TransportErrorKind::ResponseTooLarge
        );
    }

    #[test]
    fn compressed_response_is_denied() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 0\r\n\r\n";
        assert_eq!(
            parse_http_response(raw, 1024, 1024).unwrap_err().kind,
            TransportErrorKind::UnsupportedEncoding
        );
    }

    #[test]
    fn binance_time_midpoint_offset_is_deterministic() {
        let r = HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: "{\"serverTime\":10050}".into(),
        };
        let s = parse_server_time(PublicTimeVenue::BinanceUsdm, &r, 10_000, 10_020, 100).unwrap();
        assert_eq!(s.rtt_ms, 20);
        assert_eq!(s.offset_ms, 40);
        assert_eq!(s.corrected_ms(20_000), Some(20_040));
        assert!(s.usable_at(10_500, 1_000, 100));
    }

    #[test]
    fn okx_time_parser_and_staleness_gate() {
        let r = HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: r#"{"code":"0","data":[{"ts":"10005"}],"msg":""}"#.into(),
        };
        let s = parse_server_time(
            PublicTimeVenue::Okx(OkxDomain::Global),
            &r,
            10_000,
            10_010,
            100,
        )
        .unwrap();
        assert_eq!(s.offset_ms, 0);
        assert!(s.usable_at(10_100, 1_000, 100));
        assert!(!s.usable_at(12_000, 1_000, 100));
    }

    #[test]
    fn high_rtt_time_sample_is_rejected() {
        let r = HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: "{\"serverTime\":10050}".into(),
        };
        assert_eq!(
            parse_server_time(PublicTimeVenue::BinanceUsdm, &r, 10_000, 20_000, 100),
            Err(TimeError::RttTooHigh)
        );
    }

    #[test]
    fn timeout_mutation_is_single_call_unknown_not_retry() {
        let fake = FakeExecutor::error(TransportErrorKind::ReadTimeout);
        let observed = dispatch_order_once(&fake, &signed_binance_close());
        assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            observed.class,
            crate::private_rest::RestOutcomeClass::Unknown
        );
        assert!(!observed.terminal);
    }

    #[test]
    fn disconnect_mutation_is_single_call_unknown_not_retry() {
        let fake = FakeExecutor::error(TransportErrorKind::Read);
        let observed = dispatch_order_once(&fake, &signed_binance_close());
        assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            observed.class,
            crate::private_rest::RestOutcomeClass::Unknown
        );
    }

    #[test]
    fn binance_503_unknown_flows_through_transport_boundary() {
        let fake = FakeExecutor::response(
            503,
            r#"{"code":-1000,"msg":"Unknown error, please check your request or try again later."}"#,
        );
        let observed = dispatch_order_once(&fake, &signed_binance_close());
        assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            observed.class,
            crate::private_rest::RestOutcomeClass::Unknown
        );
    }

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "spotarb-r17-{name}-{}",
            StdSystemTime::now()
                .duration_since(StdUnixEpoch)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn timeout_flows_r15_r16_r17_into_durable_unknown_recovery_without_retry() {
        let root = temp_root("timeout-chain");
        let lifecycle_store = DurableLifecycleStore::open(root.join("lifecycle.json")).unwrap();
        let mut gate = DurableMutationGate::open(root.join("intents.json")).unwrap();
        let mut lifecycle = PositionLifecycle::new_hedged(10, 7).unwrap();
        lifecycle.start_close().unwrap();
        lifecycle_store.persist(&lifecycle).unwrap();

        let plan = lifecycle
            .prepare_close_mutation(
                &lifecycle_store,
                &mut gate,
                "close-fut-r17",
                "sab00000000000000000000000000024",
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
            1_700_000_000_123,
            OkxDomain::Global,
            BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
        )
        .unwrap();

        let fake = FakeExecutor::error(TransportErrorKind::ReadTimeout);
        let observed = dispatch_order_once(&fake, &signed);
        assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
        let lifecycle_observation = to_lifecycle_mutation_observation(&observed).unwrap();
        lifecycle
            .apply_network_observation(
                &lifecycle_store,
                &mut gate,
                "close-fut-r17",
                lifecycle_observation,
            )
            .unwrap();

        assert_eq!(lifecycle.state, LifecycleState::RecoveryRequired);
        assert_eq!(lifecycle.next_action(), LifecycleAction::ReconcilePending);
        assert_eq!(gate.store.load().unwrap()[0].state, IntentState::Unknown);
        assert!(DurableMutationGate::open(root.join("intents.json"))
            .unwrap()
            .begin(
                "blind-retry",
                Leg::Futures,
                "sab00000000000000000000000000025"
            )
            .is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn binance_503_unknown_flows_into_same_durable_recovery_barrier() {
        let root = temp_root("503-chain");
        let lifecycle_store = DurableLifecycleStore::open(root.join("lifecycle.json")).unwrap();
        let mut gate = DurableMutationGate::open(root.join("intents.json")).unwrap();
        let mut lifecycle = PositionLifecycle::new_hedged(10, 7).unwrap();
        lifecycle.start_close().unwrap();
        lifecycle_store.persist(&lifecycle).unwrap();

        let plan = lifecycle
            .prepare_close_mutation(
                &lifecycle_store,
                &mut gate,
                "close-fut-503",
                "sab00000000000000000000000000026",
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
            1_700_000_000_123,
            OkxDomain::Global,
            BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
        )
        .unwrap();

        let fake = FakeExecutor::response(
            503,
            r#"{"code":-1000,"msg":"Unknown error, please check your request or try again later."}"#,
        );
        let observed = dispatch_order_once(&fake, &signed);
        assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
        lifecycle
            .apply_network_observation(
                &lifecycle_store,
                &mut gate,
                "close-fut-503",
                to_lifecycle_mutation_observation(&observed).unwrap(),
            )
            .unwrap();
        assert_eq!(lifecycle.state, LifecycleState::RecoveryRequired);
        assert_eq!(gate.store.load().unwrap()[0].state, IntentState::Unknown);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn synchronized_time_feeds_signed_position_mode_request() {
        let fake_time = FakeExecutor::response(200, r#"{"serverTime":10040}"#);
        let response = fake_time
            .execute_request(&HttpsRequest::public_time(PublicTimeVenue::BinanceUsdm))
            .unwrap();
        let sample =
            parse_server_time(PublicTimeVenue::BinanceUsdm, &response, 10_000, 10_020, 100)
                .unwrap();
        assert_eq!(sample.offset_ms, 30);
        let corrected = sample.corrected_ms(20_000).unwrap();
        let mode = build_binance_position_mode_request(
            &binance_creds(),
            corrected,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
        )
        .unwrap();
        assert!(mode.url.contains("timestamp=20030"));
    }

    #[test]
    fn position_mode_read_is_one_call_and_fail_closed() {
        let req = build_binance_position_mode_request(
            &binance_creds(),
            1_700_000_000_123,
            BINANCE_DEFAULT_RECV_WINDOW_MS,
        )
        .unwrap();
        let one_way = FakeExecutor::response(200, r#"{"dualSidePosition":false}"#);
        assert_eq!(
            read_binance_position_mode_once(&one_way, &req),
            BinancePositionMode::OneWay
        );
        assert_eq!(one_way.calls.load(Ordering::SeqCst), 1);

        let timeout = FakeExecutor::error(TransportErrorKind::ReadTimeout);
        assert_eq!(
            read_binance_position_mode_once(&timeout, &req),
            BinancePositionMode::Unknown
        );
        assert_eq!(timeout.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn credential_bearing_requests_never_go_to_public_or_wrong_host() {
        let mut req = HttpsRequest::from_signed(&signed_binance_close());
        req.url = "https://www.okx.com/api/v5/trade/order".into();
        assert_eq!(
            validate_request(&req).unwrap_err().kind,
            TransportErrorKind::EndpointDenied
        );
        req.url = "https://fapi.binance.com/fapi/v1/time".into();
        assert_eq!(
            validate_request(&req).unwrap_err().kind,
            TransportErrorKind::EndpointDenied
        );
    }

    #[test]
    #[ignore = "live public read-only HTTPS gate"]
    fn live_public_time_binance() {
        let sample = fetch_public_time(&HttpsClient::default(), PublicTimeVenue::BinanceUsdm)
            .expect("binance public time");
        println!(
            "binance_public_time server_ms={} rtt_ms={} offset_ms={}",
            sample.server_ms, sample.rtt_ms, sample.offset_ms
        );
        assert!(sample.rtt_ms <= DEFAULT_MAX_TIME_SAMPLE_RTT_MS);
        assert!(sample.usable_at(
            wall_ms(),
            DEFAULT_TIME_SAMPLE_MAX_AGE_MS,
            DEFAULT_MAX_TIME_SAMPLE_RTT_MS
        ));
    }

    #[test]
    #[ignore = "live public read-only HTTPS gate"]
    fn live_public_time_okx() {
        let sample = fetch_public_time(
            &HttpsClient::default(),
            PublicTimeVenue::Okx(OkxDomain::Global),
        )
        .expect("okx public time");
        println!(
            "okx_public_time server_ms={} rtt_ms={} offset_ms={}",
            sample.server_ms, sample.rtt_ms, sample.offset_ms
        );
        assert!(sample.rtt_ms <= DEFAULT_MAX_TIME_SAMPLE_RTT_MS);
        assert!(sample.usable_at(
            wall_ms(),
            DEFAULT_TIME_SAMPLE_MAX_AGE_MS,
            DEFAULT_MAX_TIME_SAMPLE_RTT_MS
        ));
    }
}


#[cfg(test)]
mod r19_fill_history_transport_tests {
    use super::*;
    use crate::private_rest::{
        build_binance_user_trades_request, build_okx_fills_history_request,
        BinanceCredentials, OkxCredentials,
    };
    #[test]
    fn read_only_fill_history_paths_are_allowlisted_but_remain_non_mutating(){
        let ok=OkxCredentials::new("TEST_OKX_KEY","TEST_OKX_SECRET","TEST_OKX_PASS").unwrap();
        let or=build_okx_fills_history_request(&ok,1_700_000_000_123,OkxDomain::Global,"TEST/USDT","123").unwrap();
        let oh=HttpsRequest::from_signed(&or);assert!(validate_request(&oh).is_ok());assert!(!oh.mutation);
        let bn=BinanceCredentials::new("TEST_BIN_KEY","TEST_BIN_SECRET").unwrap();
        let br=build_binance_user_trades_request(&bn,1_700_000_000_123,10_000,"TEST/USDT:USDT","22").unwrap();
        let bh=HttpsRequest::from_signed(&br);assert!(validate_request(&bh).is_ok());assert!(!bh.mutation);
    }
    #[test]
    fn fill_history_paths_reject_public_or_wrong_venue_credentials(){
        let mut req=HttpsRequest{method:HttpMethod::Get,url:"https://fapi.binance.com/fapi/v1/userTrades?x=1".into(),headers:vec![],body:None,credential_bearing:false,credential_scope:None,mutation:false};
        assert_eq!(validate_request(&req).unwrap_err().kind,TransportErrorKind::EndpointDenied);
        req.credential_bearing=true;req.credential_scope=Some(Venue::OkxSpot);
        req.headers=vec![
            ("OK-ACCESS-KEY".into(),"x".into()),
            ("OK-ACCESS-SIGN".into(),format!("{}=", "A".repeat(43))),
            ("OK-ACCESS-PASSPHRASE".into(),"FAKE_PASS".into()),
            ("OK-ACCESS-TIMESTAMP".into(),"2026-10-15T12:00:00.000Z".into()),
        ];
        assert_eq!(validate_request(&req).unwrap_err().kind,TransportErrorKind::EndpointDenied);
    }
}


#[cfg(test)]
mod r20_metadata_transport_tests {
    use super::*;
    use crate::market_metadata::{binance_exchange_info_request, okx_spot_instruments_request};

    #[test]
    fn exact_public_metadata_endpoints_are_allowlisted() {
        assert!(validate_request(&binance_exchange_info_request()).is_ok());
        assert!(validate_request(&okx_spot_instruments_request()).is_ok());
    }

    #[test]
    fn metadata_endpoint_query_scope_is_fail_closed() {
        let mut b = binance_exchange_info_request();
        b.url.push_str("?symbol=BTCUSDT");
        assert_eq!(validate_request(&b).unwrap_err().kind, TransportErrorKind::EndpointDenied);
        let mut o = okx_spot_instruments_request();
        o.url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP".into();
        assert_eq!(validate_request(&o).unwrap_err().kind, TransportErrorKind::EndpointDenied);
        o.url = "https://www.okx.com/api/v5/public/instruments?instType=SPOT&instId=BTC-USDT".into();
        assert_eq!(validate_request(&o).unwrap_err().kind, TransportErrorKind::EndpointDenied);
    }
}

#[cfg(test)]
mod p108_auth_header_scope_tests {
    use super::*;
    fn public_binance()->HttpsRequest{
        HttpsRequest::public_time(PublicTimeVenue::BinanceUsdm)
    }
    fn public_okx()->HttpsRequest{
        HttpsRequest::public_time(PublicTimeVenue::Okx(OkxDomain::Global))
    }
    fn denied(req:&HttpsRequest){
        assert_eq!(validate_request(req).unwrap_err().kind,
            TransportErrorKind::InvalidHeader);
        // The normal concrete executor MUST reject before any network.
        assert_eq!(HttpsClient::default().execute(req).unwrap_err().kind,
            TransportErrorKind::InvalidHeader);
    }
    #[test]
    fn p108_public_get_cannot_smuggle_unmarked_exchange_credentials(){
        for (name,value) in [
            ("X-MBX-APIKEY","FAKE_BIN"),
            ("OK-ACCESS-KEY","FAKE_OKX"),
            ("OK-ACCESS-SIGN","FAKE_SIG"),
            ("OK-ACCESS-PASSPHRASE","FAKE_PASS"),
            ("OK-ACCESS-TIMESTAMP","FAKE_TIME")
        ]{
            let mut request=public_binance();
            request.headers.push((name.into(),value.into()));
            denied(&request);
            let mut okx=public_okx();
            okx.headers.push((name.into(),value.into()));
            denied(&okx);
        }
    }
    #[test]
    fn p108_private_scope_must_match_actual_key_header(){
        let mut req=public_binance();
        req.url=format!("https://fapi.binance.com/fapi/v3/positionRisk?timestamp=1700000000000&recvWindow=10000&signature={}", "a".repeat(64));
        req.credential_bearing=true;
        req.credential_scope=Some(Venue::BinanceUsdm);
        denied(&req); // A Boolean cannot conjure a missing key.
        req.headers.push(("X-MBX-APIKEY".into(),"FAKE_BIN".into()));
        assert!(validate_request(&req).is_ok());
        req.credential_bearing=false;
        denied(&req);
        req.credential_bearing=true;
        req.credential_scope=Some(Venue::OkxSpot);
        denied(&req);
        req.credential_scope=Some(Venue::BinanceUsdm);
        req.headers.push(("OK-ACCESS-KEY".into(),"FAKE_OKX".into()));
        denied(&req);
        let mut ok=public_okx();
        ok.url="https://www.okx.com/api/v5/account/balance".into();
        ok.credential_bearing=true;
        ok.credential_scope=Some(Venue::OkxSpot);
        ok.headers.push(("OK-ACCESS-KEY".into(),"FAKE_OKX".into()));
        assert_eq!(validate_request(&ok).unwrap_err().kind,TransportErrorKind::InvalidHeader);
        ok.headers.push(("OK-ACCESS-SIGN".into(),format!("{}=", "A".repeat(43))));
        ok.headers.push(("OK-ACCESS-PASSPHRASE".into(),"FAKE_PASS".into()));
        ok.headers.push(("OK-ACCESS-TIMESTAMP".into(),"2026-10-15T12:00:00.000Z".into()));
        assert!(validate_request(&ok).is_ok());
        ok.headers.push(("X-MBX-APIKEY".into(),"FAKE_BIN".into()));
        denied(&ok);
    }
    #[test]
    fn p108_duplicate_or_ambiguous_header_forms_denied_before_dns(){
        let mut req=public_binance();
        req.headers=vec![
           ("X-MBX-APIKEY".into(),"FAKE_BIN".into()),
           ("x-mbx-apikey".into(),"FAKE_BIN_2".into()),
        ];
        denied(&req);
        req.headers=vec![("Bad:Header".into(),"x".into())];
        denied(&req);
        req.headers=vec![("X-Latin".into(),"é".into())];
        denied(&req);
        req.headers=vec![("Cookie".into(),"secret=FAKE".into())];
        denied(&req);
        req.headers=vec![("Authorization".into(),"Bearer FAKE".into())];
        denied(&req);
        req.headers=vec![("Proxy-Authorization".into(),"FAKE".into())];
        denied(&req);
        req.headers=vec![("Accept-Encoding".into(),"gzip".into())];
        denied(&req);
        req.headers=vec![("X-Forwarded-Host".into(),"evil.test".into())];
        denied(&req);
        req.headers=vec![("X-MBX-DUPLICATE".into(),"secret".into())];
        denied(&req);
        for name in ["X-API-KEY","X-Auth-Token","X-Test","X-Session"]{
            req.headers=vec![(name.into(),"unapproved-secret".into())];
            denied(&req);
        }
        req.headers=vec![("X-Test".into(),"ok".repeat(8200))];
        denied(&req);
        req.headers=vec![("X-Test".into(),"ok".into());33];
        denied(&req);
    }
    #[test]
    fn p108_valid_private_signed_get_headers_remain_compatible(){
        use crate::private_rest::{
          BinanceCredentials,OkxCredentials,
          build_binance_balance_v3_request,build_okx_balance_request
        };
        let bn=BinanceCredentials::new("FAKE_KEY","FAKE_SECRET").unwrap();
        let ok=OkxCredentials::new("FAKE_KEY","FAKE_SECRET","FAKE_PASS").unwrap();
        let req_bn=build_binance_balance_v3_request(
          &bn,1_700_000_000_000,10_000).unwrap();
        let req_ok=build_okx_balance_request(
          &ok,1_700_000_000_000,OkxDomain::Global).unwrap();
        for signed in [&req_bn,&req_ok]{
           let req=HttpsRequest::from_signed(signed);
           assert!(validate_request(&req).is_ok());
           assert!(inspect_readonly_get_policy_without_network(&req).is_ok());
        }
    }
    #[test]
    fn p108_public_exchange_clock_endpoints_have_no_query_surface(){
        for mut req in [public_binance(),public_okx()]{
            assert!(validate_request(&req).is_ok());
            req.url.push_str("?api_key=FAKE_SECRET");
            assert_eq!(validate_request(&req).unwrap_err().kind,
               TransportErrorKind::EndpointDenied);
            assert_eq!(HttpsClient::default().execute(&req).unwrap_err().kind,
               TransportErrorKind::EndpointDenied);
        }
    }
}

#[cfg(test)]
mod p108_complete_signed_get_shape_regression {
    use super::*;
    use crate::private_rest::{
        BinanceCredentials,OkxCredentials,build_binance_balance_v3_request,
        build_okx_balance_request,
    };
    fn binance()->HttpsRequest{
        let creds=BinanceCredentials::new("FAKE_BIN_KEY","FAKE_BIN_SECRET").unwrap();
        HttpsRequest::from_signed(&build_binance_balance_v3_request(
            &creds,1_700_000_000_123,10_000).unwrap())
    }
    fn okx()->HttpsRequest{
        let creds=OkxCredentials::new("FAKE_OKX_KEY","FAKE_OKX_SECRET","FAKE_PASS").unwrap();
        HttpsRequest::from_signed(&build_okx_balance_request(
            &creds,1_700_000_000_123,OkxDomain::Global).unwrap())
    }
    fn denied(request:&HttpsRequest){
        assert_eq!(validate_request(request).unwrap_err().kind,
            TransportErrorKind::InvalidHeader);
        // Invalid requests fail before the concrete executor can reach DNS.
        assert_eq!(HttpsClient::default().execute(request).unwrap_err().kind,
            TransportErrorKind::InvalidHeader);
    }
    #[test]
    fn p108_okx_requires_all_four_declared_auth_headers_even_for_read_only_get(){
        let valid=okx();
        assert!(validate_request(&valid).is_ok());
        for forbidden in ["OK-ACCESS-KEY","OK-ACCESS-SIGN",
                          "OK-ACCESS-PASSPHRASE","OK-ACCESS-TIMESTAMP"] {
            let mut broken=valid.clone();
            broken.headers.retain(|(name,_)|name!=forbidden);
            denied(&broken);
        }
        let mut wrong=valid.clone();
        wrong.credential_scope=Some(Venue::BinanceUsdm);
        denied(&wrong);
        for (header,bad) in [
            ("OK-ACCESS-SIGN","NOT_BASE64"),
            ("OK-ACCESS-SIGN","A!AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            ("OK-ACCESS-TIMESTAMP","1700000000"),
            ("OK-ACCESS-TIMESTAMP","2026-10-15 12:00:00.000Z"),
        ]{
            let mut malformed=valid.clone();
            for (name,value) in &mut malformed.headers{
                if name==header{*value=bad.into();}
            }
            denied(&malformed);
        }
    }
    #[test]
    fn p108_binance_private_get_needs_unique_bounded_signed_query_fields(){
        let valid=binance();
        assert!(validate_request(&valid).is_ok());
        for removal in ["&signature=","&recvWindow=","timestamp="] {
            let mut broken=valid.clone();
            let q=Url::parse(&valid.url).unwrap();
            let remaining=q.query_pairs().filter(|(name,_)|{
                match removal {
                    "&signature="=>name!="signature",
                    "&recvWindow="=>name!="recvWindow",
                    _=>name!="timestamp"
                }
            }).map(|(k,v)|format!("{k}={v}")).collect::<Vec<_>>().join("&");
            broken.url=format!("https://fapi.binance.com/fapi/v3/balance?{remaining}");
            denied(&broken);
        }
        for addition in ["&timestamp=1700000000001","&signature=deadbeef",
                         "&recvWindow=10000","&recvWindow=60001"]{
            let mut broken=valid.clone();
            broken.url.push_str(addition);
            denied(&broken);
        }
        let mut unsigned=valid.clone();
        unsigned.url=unsigned.url.replace("signature=","not_signature=");
        denied(&unsigned);
        let mut wrong_case=valid.clone();
        wrong_case.url=wrong_case.url.replace("signature=","Signature=");
        denied(&wrong_case);
    }
}
