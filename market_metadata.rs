//! R20 / R8 debt closure: pure-native public exchange metadata acquisition.
//!
//! No credentials and no trading. Two bounded HTTPS reads are parsed into one
//! immutable canonical snapshot. A caller publishes it only after both venues
//! have succeeded and the complete snapshot has validated.

use crate::{
    decision::{Limits, RouteSpec},
    private_https::{wall_ms, HttpExecutor, HttpsClient, HttpsRequest, TransportErrorKind},
    private_rest::HttpMethod,
    transport::monotonic_ns,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

pub const METADATA_CONNECT_TIMEOUT_MS: u64 = 3_000;
pub const METADATA_IO_TIMEOUT_MS: u64 = 8_000;
pub const METADATA_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub const ROUTE_LABEL: &str = "OKX Spot BUY -> Binance Futures SHORT";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataError {
    InvalidGeneration,
    Transport(TransportErrorKind),
    HttpStatus(u16),
    Json,
    InvalidEnvelope,
    DuplicateBase,
    EmptyIntersection,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeMarketMetadata {
    pub base: String,
    pub spot_symbol: String,
    pub future_symbol: String,
    pub spot_min_amount: Option<String>,
    pub future_min_base: Option<String>,
    pub spot_min_cost: String,
    pub future_min_cost: String,
    pub spot_step: Option<String>,
    pub future_step_base: Option<String>,
    pub contract_size: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub route_label: String,
    pub static_reason: Option<String>,
    pub loaded_ns: u64,
    pub loaded_wall_ms: u64,
    pub spot_tick: Option<String>,
    pub future_tick: Option<String>,
    pub spot_status: String,
    pub future_status: String,
    pub spot_max_amount: Option<String>,
    pub future_max_base: Option<String>,
}

impl NativeMarketMetadata {
    pub fn metadata_state(&self) -> &'static str {
        match self.static_reason.as_deref() {
            Some("MARKET_INACTIVE") => "INSTRUMENT_INACTIVE",
            Some("EXCHANGE_MINIMUM_UNKNOWN") => "SIZE_METADATA_MISSING",
            Some(_) => "SIZE_METADATA_INVALID",
            None if self.loaded_ns == 0 => "SIZE_METADATA_INVALID",
            None => "SIZE_METADATA_READY",
        }
    }

    pub fn route_spec(&self) -> RouteSpec {
        RouteSpec {
            buy_exchange: self.buy_exchange.clone(),
            sell_exchange: self.sell_exchange.clone(),
            label: self.route_label.clone(),
            buy: Limits {
                min_amount: self.spot_min_amount.clone().unwrap_or_default(),
                step: self.spot_step.clone().unwrap_or_default(),
                min_cost: self.spot_min_cost.clone(),
                max_amount: self.spot_max_amount.clone(),
            },
            sell: Limits {
                min_amount: self.future_min_base.clone().unwrap_or_default(),
                step: self.future_step_base.clone().unwrap_or_default(),
                min_cost: self.future_min_cost.clone(),
                max_amount: self.future_max_base.clone(),
            },
            loaded_ns: self.loaded_ns,
            metadata_state: self.metadata_state().into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NativeMetadataSnapshot {
    pub generation: u64,
    pub loaded_ns: u64,
    pub loaded_wall_ms: u64,
    pub binance_active: usize,
    pub binance_tradifi_excluded: usize,
    pub okx_active: usize,
    pub metadata_ready: usize,
    pub static_denied: usize,
    pub markets: BTreeMap<String, NativeMarketMetadata>,
    pub routes: HashMap<String, Vec<RouteSpec>>,
}

impl NativeMetadataSnapshot {
    pub fn intersection(&self) -> usize {
        self.markets.len()
    }

    pub fn bases(&self) -> Vec<String> {
        self.markets.keys().cloned().collect()
    }

    pub fn canonical_control_value(&self) -> Value {
        let routes:BTreeMap<String,Vec<RouteSpec>>=self.routes.iter().map(|(k,v)|(k.clone(),v.clone())).collect();
        serde_json::json!({"generation":self.generation,"routes":routes})
    }
}

#[derive(Clone, Debug)]
struct ParsedBinance {
    markets:BTreeMap<String,BinanceInstrument>,
    tradifi_excluded:usize,
}

#[derive(Clone, Debug)]
struct BinanceInstrument {
    symbol: String,
    status: String,
    min_amount: Option<String>,
    step: Option<String>,
    max_amount: Option<String>,
    tick: Option<String>,
    min_cost: String,
}

#[derive(Clone, Debug)]
struct OkxInstrument {
    symbol: String,
    status: String,
    min_amount: Option<String>,
    step: Option<String>,
    tick: Option<String>,
}

pub fn binance_exchange_info_request() -> HttpsRequest {
    HttpsRequest {
        method: HttpMethod::Get,
        url: "https://fapi.binance.com/fapi/v1/exchangeInfo".into(),
        headers: Vec::new(),
        body: None,
        credential_bearing: false,
        credential_scope: None,
        mutation: false,
    }
}

pub fn binance_spot_exchange_info_request() -> HttpsRequest {
    HttpsRequest {
        method: HttpMethod::Get,
        url: "https://api.binance.com/api/v3/exchangeInfo".into(),
        headers: Vec::new(),
        body: None,
        credential_bearing: false,
        credential_scope: None,
        mutation: false,
    }
}

pub fn okx_spot_instruments_request() -> HttpsRequest {
    HttpsRequest {
        method: HttpMethod::Get,
        url: "https://www.okx.com/api/v5/public/instruments?instType=SPOT".into(),
        headers: Vec::new(),
        body: None,
        credential_bearing: false,
        credential_scope: None,
        mutation: false,
    }
}

pub fn metadata_https_client() -> Result<HttpsClient, MetadataError> {
    HttpsClient::new(
        Duration::from_millis(METADATA_CONNECT_TIMEOUT_MS),
        Duration::from_millis(METADATA_IO_TIMEOUT_MS),
        METADATA_MAX_RESPONSE_BYTES,
    )
    .map_err(|e| MetadataError::Transport(e.kind))
}

pub fn fetch_current_metadata(generation: u64) -> Result<NativeMetadataSnapshot, MetadataError> {
    let client = metadata_https_client()?;
    fetch_metadata_with(&client, generation)
}

pub fn fetch_metadata_with<E: HttpExecutor>(
    executor: &E,
    generation: u64,
) -> Result<NativeMetadataSnapshot, MetadataError> {
    if generation == 0 {
        return Err(MetadataError::InvalidGeneration);
    }
    let binance = executor
        .execute_request(&binance_exchange_info_request())
        .map_err(|e| MetadataError::Transport(e.kind))?;
    if binance.status != 200 {
        return Err(MetadataError::HttpStatus(binance.status));
    }
    let okx = executor
        .execute_request(&okx_spot_instruments_request())
        .map_err(|e| MetadataError::Transport(e.kind))?;
    if okx.status != 200 {
        return Err(MetadataError::HttpStatus(okx.status));
    }
    let loaded_ns = monotonic_ns();
    let loaded_wall_ms = wall_ms();
    build_snapshot(
        &binance.body,
        &okx.body,
        generation,
        loaded_ns,
        loaded_wall_ms,
    )
}

pub fn fetch_current_spot_spot_metadata(generation: u64) -> Result<NativeMetadataSnapshot, MetadataError> {
    let client = metadata_https_client()?;
    fetch_spot_spot_metadata_with(&client, generation)
}

pub fn fetch_spot_spot_metadata_with<E: HttpExecutor>(
    executor: &E,
    generation: u64,
) -> Result<NativeMetadataSnapshot, MetadataError> {
    if generation == 0 {
        return Err(MetadataError::InvalidGeneration);
    }
    let binance = executor
        .execute_request(&binance_spot_exchange_info_request())
        .map_err(|e| MetadataError::Transport(e.kind))?;
    if binance.status != 200 {
        return Err(MetadataError::HttpStatus(binance.status));
    }
    let okx = executor
        .execute_request(&okx_spot_instruments_request())
        .map_err(|e| MetadataError::Transport(e.kind))?;
    if okx.status != 200 {
        return Err(MetadataError::HttpStatus(okx.status));
    }
    build_spot_spot_snapshot(&binance.body, &okx.body, generation, monotonic_ns(), wall_ms())
}

pub fn build_spot_spot_snapshot(
    binance_body: &str,
    okx_body: &str,
    generation: u64,
    loaded_ns: u64,
    loaded_wall_ms: u64,
) -> Result<NativeMetadataSnapshot, MetadataError> {
    if generation == 0 || loaded_ns == 0 {
        return Err(MetadataError::InvalidGeneration);
    }
    let binance = parse_binance_spot(binance_body)?;
    let okx = parse_okx(okx_body)?;
    let mut markets = BTreeMap::new();
    for (base, binance_spot) in &binance.markets {
        let Some(okx_spot) = okx.get(base) else { continue };
        let static_reason = if [
            okx_spot.min_amount.as_ref(),
            okx_spot.step.as_ref(),
            binance_spot.min_amount.as_ref(),
            binance_spot.step.as_ref(),
        ]
        .iter()
        .any(|value| value.is_none())
        {
            Some("EXCHANGE_MINIMUM_UNKNOWN".to_string())
        } else if okx_spot.status != "live" || binance_spot.status != "TRADING" {
            Some("MARKET_INACTIVE".to_string())
        } else {
            None
        };
        markets.insert(base.clone(), NativeMarketMetadata {
            base: base.clone(),
            spot_symbol: okx_spot.symbol.clone(),
            future_symbol: binance_spot.symbol.clone(),
            spot_min_amount: okx_spot.min_amount.clone(),
            future_min_base: binance_spot.min_amount.clone(),
            spot_min_cost: "0".into(),
            future_min_cost: binance_spot.min_cost.clone(),
            spot_step: okx_spot.step.clone(),
            future_step_base: binance_spot.step.clone(),
            contract_size: "1".into(),
            buy_exchange: "okx_spot".into(),
            sell_exchange: "binance_spot".into(),
            route_label: "OKX Spot BUY -> Binance Spot SELL".into(),
            static_reason,
            loaded_ns,
            loaded_wall_ms,
            spot_tick: okx_spot.tick.clone(),
            future_tick: binance_spot.tick.clone(),
            spot_status: okx_spot.status.clone(),
            future_status: binance_spot.status.clone(),
            spot_max_amount: None,
            future_max_base: binance_spot.max_amount.clone(),
        });
    }
    if markets.is_empty() { return Err(MetadataError::EmptyIntersection); }
    let metadata_ready = markets.values()
        .filter(|market| market.metadata_state() == "SIZE_METADATA_READY")
        .count();
    let static_denied = markets.len() - metadata_ready;
    let routes = markets.iter()
        .map(|(base, metadata)| (base.clone(), vec![metadata.route_spec()]))
        .collect();
    Ok(NativeMetadataSnapshot {
        generation,
        loaded_ns,
        loaded_wall_ms,
        binance_active: binance.markets.len(),
        binance_tradifi_excluded: 0,
        okx_active: okx.len(),
        metadata_ready,
        static_denied,
        markets,
        routes,
    })
}

pub fn build_snapshot(
    binance_body: &str,
    okx_body: &str,
    generation: u64,
    loaded_ns: u64,
    loaded_wall_ms: u64,
) -> Result<NativeMetadataSnapshot, MetadataError> {
    if generation == 0 || loaded_ns == 0 {
        return Err(MetadataError::InvalidGeneration);
    }
    let binance = parse_binance(binance_body)?;
    let okx = parse_okx(okx_body)?;

    let mut markets = BTreeMap::new();
    for (base, future) in &binance.markets {
        let Some(spot) = okx.get(base) else { continue };
        let static_reason = if [
            spot.min_amount.as_ref(),
            spot.step.as_ref(),
            future.min_amount.as_ref(),
            future.step.as_ref(),
        ]
        .iter()
        .any(|x| x.is_none())
        {
            Some("EXCHANGE_MINIMUM_UNKNOWN".to_string())
        } else {
            None
        };
        markets.insert(
            base.clone(),
            NativeMarketMetadata {
                base: base.clone(),
                spot_symbol: spot.symbol.clone(),
                future_symbol: future.symbol.clone(),
                spot_min_amount: spot.min_amount.clone(),
                future_min_base: future.min_amount.clone(),
                spot_min_cost: "0".into(),
                future_min_cost: future.min_cost.clone(),
                spot_step: spot.step.clone(),
                future_step_base: future.step.clone(),
                contract_size: "1".into(),
                buy_exchange: "okx_spot".into(),
                sell_exchange: "binance_futures".into(),
                route_label: ROUTE_LABEL.into(),
                static_reason,
                loaded_ns,
                loaded_wall_ms,
                spot_tick: spot.tick.clone(),
                future_tick: future.tick.clone(),
                spot_status: spot.status.clone(),
                future_status: future.status.clone(),
                spot_max_amount: None,
                future_max_base: future.max_amount.clone(),
            },
        );
    }
    if markets.is_empty() {
        return Err(MetadataError::EmptyIntersection);
    }

    let metadata_ready = markets
        .values()
        .filter(|m| m.metadata_state() == "SIZE_METADATA_READY")
        .count();
    let static_denied = markets.len() - metadata_ready;
    let routes = markets
        .iter()
        .map(|(base, metadata)| (base.clone(), vec![metadata.route_spec()]))
        .collect();

    Ok(NativeMetadataSnapshot {
        generation,
        loaded_ns,
        loaded_wall_ms,
        binance_active: binance.markets.len(),
        binance_tradifi_excluded: binance.tradifi_excluded,
        okx_active: okx.len(),
        metadata_ready,
        static_denied,
        markets,
        routes,
    })
}

fn parse_binance(body: &str) -> Result<ParsedBinance, MetadataError> {
    let root: Value = serde_json::from_str(body).map_err(|_| MetadataError::Json)?;
    let symbols = root
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or(MetadataError::InvalidEnvelope)?;
    let mut out = BTreeMap::new();
    let mut tradifi_excluded=0usize;

    for row in symbols {
        if text(row, "quoteAsset") != Some("USDT") || text(row, "status") != Some("TRADING") { continue; }
        let tradifi = text(row,"contractType") == Some("TRADIFI_PERPETUAL")
            || text(row,"underlyingType") == Some("EQUITY")
            || row.get("underlyingSubType").and_then(Value::as_array)
                .is_some_and(|xs| xs.iter().any(|x|x.as_str()==Some("TradFi")));
        if tradifi { tradifi_excluded+=1; continue; }
        if text(row, "contractType") != Some("PERPETUAL") { continue; }
        let base = nonempty(row, "baseAsset").ok_or(MetadataError::InvalidEnvelope)?;
        let symbol = nonempty(row, "symbol").ok_or(MetadataError::InvalidEnvelope)?;
        if out.contains_key(&base) {
            return Err(MetadataError::DuplicateBase);
        }
        let filters = row
            .get("filters")
            .and_then(Value::as_array)
            .ok_or(MetadataError::InvalidEnvelope)?;
        let lot = find_filter(filters, "LOT_SIZE");
        let market_lot = find_filter(filters, "MARKET_LOT_SIZE");
        let price_filter = find_filter(filters, "PRICE_FILTER");
        let min_notional = find_filter(filters, "MIN_NOTIONAL")
            .or_else(|| find_filter(filters, "NOTIONAL"));

        let min_amount = greatest([
            decimal_field(market_lot, "minQty"),
            decimal_field(lot, "minQty"),
        ]);
        let step = greatest([
            decimal_field(market_lot, "stepSize"),
            decimal_field(lot, "stepSize"),
        ]);
        let max_amount = least([
            decimal_field(market_lot, "maxQty"),
            decimal_field(lot, "maxQty"),
        ]);
        let tick = decimal_field(price_filter, "tickSize").map(|(_, raw)| raw);
        let min_cost = greatest([
            decimal_field(min_notional, "notional"),
            decimal_field(min_notional, "minNotional"),
        ])
        .unwrap_or_else(|| "0".into());

        out.insert(
            base,
            BinanceInstrument {
                symbol,
                status: "TRADING".into(),
                min_amount,
                step,
                max_amount,
                tick,
                min_cost,
            },
        );
    }
    Ok(ParsedBinance{markets:out,tradifi_excluded})
}

fn parse_binance_spot(body: &str) -> Result<ParsedBinance, MetadataError> {
    let root: Value = serde_json::from_str(body).map_err(|_| MetadataError::Json)?;
    let symbols = root
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or(MetadataError::InvalidEnvelope)?;
    let mut out = BTreeMap::new();

    for row in symbols {
        if text(row, "quoteAsset") != Some("USDT")
            || text(row, "status") != Some("TRADING")
        {
            continue;
        }
        let base = nonempty(row, "baseAsset").ok_or(MetadataError::InvalidEnvelope)?;
        let symbol = nonempty(row, "symbol").ok_or(MetadataError::InvalidEnvelope)?;
        if out.contains_key(&base) {
            return Err(MetadataError::DuplicateBase);
        }
        let filters = row
            .get("filters")
            .and_then(Value::as_array)
            .ok_or(MetadataError::InvalidEnvelope)?;
        let lot = find_filter(filters, "LOT_SIZE");
        let market_lot = find_filter(filters, "MARKET_LOT_SIZE");
        let price_filter = find_filter(filters, "PRICE_FILTER");
        let min_notional = find_filter(filters, "MIN_NOTIONAL")
            .or_else(|| find_filter(filters, "NOTIONAL"));
        let min_amount = greatest([
            decimal_field(lot, "minQty"),
            decimal_field(market_lot, "minQty"),
        ]);
        let step = greatest([
            decimal_field(lot, "stepSize"),
            decimal_field(market_lot, "stepSize"),
        ]);
        let max_amount = least([
            decimal_field(lot, "maxQty"),
            decimal_field(market_lot, "maxQty"),
        ]);
        let tick = decimal_field(price_filter, "tickSize").map(|(_, raw)| raw);
        let min_cost = greatest([
            decimal_field(min_notional, "minNotional"),
            decimal_field(min_notional, "notional"),
        ])
        .unwrap_or_else(|| "0".into());

        out.insert(
            base,
            BinanceInstrument {
                symbol,
                status: "TRADING".into(),
                min_amount,
                step,
                max_amount,
                tick,
                min_cost,
            },
        );
    }
    Ok(ParsedBinance {
        markets: out,
        tradifi_excluded: 0,
    })
}

fn parse_okx(body: &str) -> Result<BTreeMap<String, OkxInstrument>, MetadataError> {
    let root: Value = serde_json::from_str(body).map_err(|_| MetadataError::Json)?;
    if text(&root, "code") != Some("0") {
        return Err(MetadataError::InvalidEnvelope);
    }
    let data = root
        .get("data")
        .and_then(Value::as_array)
        .ok_or(MetadataError::InvalidEnvelope)?;
    let mut out = BTreeMap::new();

    for row in data {
        if text(row, "instType") != Some("SPOT")
            || text(row, "quoteCcy") != Some("USDT")
            || text(row, "state") != Some("live")
        {
            continue;
        }
        let base = nonempty(row, "baseCcy").ok_or(MetadataError::InvalidEnvelope)?;
        let symbol = nonempty(row, "instId").ok_or(MetadataError::InvalidEnvelope)?;
        if out.contains_key(&base) {
            return Err(MetadataError::DuplicateBase);
        }
        out.insert(
            base,
            OkxInstrument {
                symbol,
                status: "live".into(),
                min_amount: decimal_field(Some(row), "minSz").map(|(_, raw)| raw),
                step: decimal_field(Some(row), "lotSz").map(|(_, raw)| raw),
                tick: decimal_field(Some(row), "tickSz").map(|(_, raw)| raw),
            },
        );
    }
    Ok(out)
}

fn text<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str)
}

fn nonempty(v: &Value, key: &str) -> Option<String> {
    text(v, key)
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .map(str::to_owned)
}

fn find_filter<'a>(filters: &'a [Value], kind: &str) -> Option<&'a Value> {
    filters
        .iter()
        .find(|row| text(row, "filterType") == Some(kind))
}

fn decimal_field(v: Option<&Value>, key: &str) -> Option<(u128, String)> {
    let raw = text(v?, key)?.trim();
    let atoms = positive_atoms(raw)?;
    Some((atoms, raw.to_owned()))
}

fn greatest<const N: usize>(values: [Option<(u128, String)>; N]) -> Option<String> {
    values
        .into_iter()
        .flatten()
        .max_by_key(|(atoms, _)| *atoms)
        .map(|(_, raw)| raw)
}

fn least<const N: usize>(values: [Option<(u128, String)>; N]) -> Option<String> {
    values
        .into_iter()
        .flatten()
        .min_by_key(|(atoms, _)| *atoms)
        .map(|(_, raw)| raw)
}

fn positive_atoms(raw: &str) -> Option<u128> {
    let raw = raw.trim();
    if raw.is_empty() || raw.starts_with('-') || raw.starts_with('+') {
        return None;
    }
    let (mantissa, exponent) = if let Some((m, e)) = raw.split_once(['e', 'E']) {
        (m, e.parse::<i32>().ok()?)
    } else {
        (raw, 0)
    };
    let (whole, frac_raw) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty()
        || !whole.bytes().all(|c| c.is_ascii_digit())
        || !frac_raw.bytes().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let frac = frac_raw.trim_end_matches('0');
    let frac_len = frac.len() as i32;
    let whole_value = whole.parse::<u128>().ok()?;
    let frac_value = if frac.is_empty() {
        0
    } else {
        frac.parse::<u128>().ok()?
    };
    let frac_scale = 10_u128.checked_pow(frac.len() as u32)?;
    let digits = whole_value
        .checked_mul(frac_scale)?
        .checked_add(frac_value)?;
    let shift = 12_i32.checked_sub(frac_len)?.checked_add(exponent)?;
    if shift < 0 || shift > 38 {
        return None;
    }
    let atoms = digits.checked_mul(10_u128.checked_pow(shift as u32)?)?;
    (atoms > 0).then_some(atoms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        decision::{DecisionConfig, DecisionCore},
        private_https::{HttpResponse, TransportError},
    };
    use std::{
        collections::{HashSet, VecDeque},
        sync::Mutex,
    };

    const BINANCE: &str = r#"{
      "symbols":[
        {"symbol":"AAAUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"AAA","quoteAsset":"USDT",
         "filters":[
          {"filterType":"PRICE_FILTER","tickSize":"0.0100"},
          {"filterType":"LOT_SIZE","minQty":"1","maxQty":"1000","stepSize":"0.1"},
          {"filterType":"MARKET_LOT_SIZE","minQty":"2","maxQty":"500","stepSize":"0.5"},
          {"filterType":"MIN_NOTIONAL","notional":"5"}
         ]},
        {"symbol":"BBBUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"BBB","quoteAsset":"USDT","filters":[]},
        {"symbol":"OLDUSDT","contractType":"PERPETUAL","status":"SETTLING","baseAsset":"OLD","quoteAsset":"USDT","filters":[]},
        {"symbol":"AAAUSD","contractType":"PERPETUAL","status":"TRADING","baseAsset":"AAA","quoteAsset":"USD","filters":[]}
      ]
    }"#;

    const OKX: &str = r#"{
      "code":"0","msg":"","data":[
        {"instId":"AAA-USDT","instType":"SPOT","baseCcy":"AAA","quoteCcy":"USDT","state":"live","lotSz":"0.2","minSz":"3","tickSz":"0.001"},
        {"instId":"BBB-USDT","instType":"SPOT","baseCcy":"BBB","quoteCcy":"USDT","state":"live","lotSz":"0.1","minSz":"","tickSz":"0.001"},
        {"instId":"OLD-USDT","instType":"SPOT","baseCcy":"OLD","quoteCcy":"USDT","state":"suspend","lotSz":"1","minSz":"1","tickSz":"0.1"},
        {"instId":"AAA-USDC","instType":"SPOT","baseCcy":"AAA","quoteCcy":"USDC","state":"live","lotSz":"1","minSz":"1","tickSz":"0.1"}
      ]
    }"#;

    #[test]
    fn exact_public_requests_are_noncredentialed_and_nonmutating() {
        let b = binance_exchange_info_request();
        assert_eq!(b.method, HttpMethod::Get);
        assert_eq!(b.url, "https://fapi.binance.com/fapi/v1/exchangeInfo");
        assert!(!b.credential_bearing && !b.mutation);
        let bs = binance_spot_exchange_info_request();
        assert_eq!(bs.method, HttpMethod::Get);
        assert_eq!(bs.url, "https://api.binance.com/api/v3/exchangeInfo");
        assert!(!bs.credential_bearing && !bs.mutation);
        let o = okx_spot_instruments_request();
        assert_eq!(
            o.url,
            "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
        );
        assert!(!o.credential_bearing && !o.mutation);
    }

    #[test]
    fn parser_matches_python_greatest_min_step_and_least_max_semantics() {
        let snap = build_snapshot(BINANCE, OKX, 1, 100, 200).unwrap();
        assert_eq!(snap.binance_active, 2);
        assert_eq!(snap.binance_tradifi_excluded, 0);
        assert_eq!(snap.okx_active, 2);
        assert_eq!(snap.intersection(), 2);
        let a = &snap.markets["AAA"];
        assert_eq!(a.spot_min_amount.as_deref(), Some("3"));
        assert_eq!(a.spot_step.as_deref(), Some("0.2"));
        assert_eq!(a.future_min_base.as_deref(), Some("2"));
        assert_eq!(a.future_step_base.as_deref(), Some("0.5"));
        assert_eq!(a.future_max_base.as_deref(), Some("500"));
        assert_eq!(a.future_tick.as_deref(), Some("0.0100"));
        assert_eq!(a.future_min_cost, "5");
        assert_eq!(a.contract_size, "1");
        assert_eq!(a.metadata_state(), "SIZE_METADATA_READY");
        let b = &snap.markets["BBB"];
        assert_eq!(b.metadata_state(), "SIZE_METADATA_MISSING");
        assert_eq!(snap.metadata_ready, 1);
        assert_eq!(snap.static_denied, 1);
    }

    #[test]
    fn spot_spot_snapshot_uses_spot_route_and_spot_exchange_constraints() {
        let binance_spot = BINANCE.replace("contractType", "spotType");
        let snap = build_spot_spot_snapshot(&binance_spot, OKX, 11, 100, 200).unwrap();
        let route = &snap.markets["AAA"];
        assert_eq!(route.buy_exchange, "okx_spot");
        assert_eq!(route.sell_exchange, "binance_spot");
        assert_eq!(route.route_label, "OKX Spot BUY -> Binance Spot SELL");
        assert_eq!(route.future_symbol, "AAAUSDT");
        assert_eq!(route.future_min_base.as_deref(), Some("2"));
        assert_eq!(route.future_step_base.as_deref(), Some("0.5"));
        assert_eq!(route.future_max_base.as_deref(), Some("500"));
        assert_eq!(route.future_min_cost, "5");
        assert_eq!(route.metadata_state(), "SIZE_METADATA_READY");
        assert_eq!(snap.routes["AAA"][0].sell_exchange, "binance_spot");
        assert_eq!(snap.markets["BBB"].metadata_state(), "SIZE_METADATA_MISSING");
    }

    #[test]
    fn route_snapshot_is_accepted_atomically_by_decision_core() {
        let snap = build_snapshot(BINANCE, OKX, 7, 100, 200).unwrap();
        let cfg = DecisionConfig {
            max_age_ms: 800,
            metadata_ttl_ms: 43_200_000,
            base_size: 10.0,
            risk_cap: 10.0,
            cost: 24.0,
            required_net: 42.0,
            min_price: 0.0,
            max_price: f64::INFINITY,
            blacklist: HashSet::new(),
        };
        let mut core = DecisionCore::new(0, 1, cfg, HashMap::new()).unwrap();
        core.stage_routes(snap.generation, snap.routes.clone()).unwrap();
        core.pause_decisions();
        core.commit_staged(7).unwrap();
        core.resume_decisions(7).unwrap();
        assert_eq!(core.metadata_generation(), 7);
        assert!(!core.decisions_paused());
    }

    #[test]
    fn canonical_control_snapshot_roundtrips_into_route_contract() {
        let snap=build_snapshot(BINANCE,OKX,11,100,200).unwrap();
        let value=snap.canonical_control_value();
        assert_eq!(value["generation"],11);
        let routes:HashMap<String,Vec<RouteSpec>>=serde_json::from_value(value["routes"].clone()).unwrap();
        assert_eq!(routes.len(),2);
        assert_eq!(routes["AAA"][0].metadata_state,"SIZE_METADATA_READY");
        assert_eq!(routes["BBB"][0].metadata_state,"SIZE_METADATA_MISSING");
    }

    #[test]
    fn same_ticker_tradifi_future_is_excluded_from_crypto_spot_intersection() {
        let b=r#"{"symbols":[{"symbol":"CATUSDT","contractType":"TRADIFI_PERPETUAL","status":"TRADING","baseAsset":"CAT","quoteAsset":"USDT","underlyingType":"EQUITY","underlyingSubType":["TradFi"],"filters":[{"filterType":"LOT_SIZE","minQty":"0.01","maxQty":"30000","stepSize":"0.01"}]}]}"#;
        let parsed=parse_binance(b).unwrap();
        assert_eq!(parsed.markets.len(),0);assert_eq!(parsed.tradifi_excluded,1);
        let o=r#"{"code":"0","data":[{"instId":"CAT-USDT","instType":"SPOT","baseCcy":"CAT","quoteCcy":"USDT","state":"live","lotSz":"1","minSz":"1","tickSz":"0.1"}]}"#;
        assert!(matches!(build_snapshot(b,o,1,1,1),Err(MetadataError::EmptyIntersection)));
    }

    #[test]
    fn duplicate_active_base_fails_closed() {
        let okx = OKX.replace(
            r#"{"instId":"BBB-USDT""#,
            r#"{"instId":"AAA2-USDT","instType":"SPOT","baseCcy":"AAA","quoteCcy":"USDT","state":"live","lotSz":"1","minSz":"1","tickSz":"0.1"},{"instId":"BBB-USDT""#,
        );
        assert!(matches!(build_snapshot(BINANCE, &okx, 1, 1, 1), Err(MetadataError::DuplicateBase)));
    }

    #[test]
    fn malformed_or_empty_payloads_fail_closed() {
        assert!(matches!(build_snapshot("{}", OKX, 1, 1, 1), Err(MetadataError::InvalidEnvelope)));
        assert!(matches!(build_snapshot(BINANCE, r#"{"code":"1","data":[]}"#, 1, 1, 1), Err(MetadataError::InvalidEnvelope)));
        assert!(matches!(build_snapshot(BINANCE, r#"{"code":"0","data":[]}"#, 1, 1, 1), Err(MetadataError::EmptyIntersection)));
        assert!(matches!(build_snapshot(BINANCE, OKX, 0, 1, 1), Err(MetadataError::InvalidGeneration)));
    }

    #[test]
    fn decimal_parser_is_exact_and_rejects_unsupported_precision() {
        assert_eq!(positive_atoms("1"), Some(1_000_000_000_000));
        assert_eq!(positive_atoms("0.00000001"), Some(10_000));
        assert_eq!(positive_atoms("0.0000100"), Some(10_000_000));
        assert_eq!(positive_atoms("1e-3"), Some(1_000_000_000));
        assert_eq!(positive_atoms("0"), None);
        assert_eq!(positive_atoms("-1"), None);
        assert_eq!(positive_atoms("0.0000000000001"), None);
    }

    struct Fake {
        responses: Mutex<VecDeque<Result<HttpResponse, TransportError>>>,
        urls: Mutex<Vec<String>>,
    }

    impl Fake {
        fn ok() -> Self {
            Self {
                responses: Mutex::new(
                    vec![
                        Ok(HttpResponse {
                            status: 200,
                            headers: vec![],
                            body: BINANCE.into(),
                        }),
                        Ok(HttpResponse {
                            status: 200,
                            headers: vec![],
                            body: OKX.into(),
                        }),
                    ]
                    .into(),
                ),
                urls: Mutex::new(Vec::new()),
            }
        }
    }

    impl HttpExecutor for Fake {
        fn execute_request(
            &self,
            request: &HttpsRequest,
        ) -> Result<HttpResponse, TransportError> {
            self.urls.lock().unwrap().push(request.url.clone());
            self.responses.lock().unwrap().pop_front().unwrap()
        }
    }

    #[test]
    fn fetch_is_two_read_only_calls_and_publishes_only_complete_snapshot() {
        let fake = Fake::ok();
        let snap = fetch_metadata_with(&fake, 9).unwrap();
        assert_eq!(snap.generation, 9);
        assert_eq!(fake.urls.lock().unwrap().len(), 2);
        assert_eq!(
            fake.urls.lock().unwrap()[0],
            "https://fapi.binance.com/fapi/v1/exchangeInfo"
        );
        assert_eq!(
            fake.urls.lock().unwrap()[1],
            "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
        );
    }

    #[test]
    fn second_venue_failure_returns_no_partial_snapshot_and_has_no_retry() {
        let fake = Fake {
            responses: Mutex::new(
                vec![
                    Ok(HttpResponse {
                        status: 200,
                        headers: vec![],
                        body: BINANCE.into(),
                    }),
                    Ok(HttpResponse {
                        status: 503,
                        headers: vec![],
                        body: "{}".into(),
                    }),
                ]
                .into(),
            ),
            urls: Mutex::new(Vec::new()),
        };
        assert!(matches!(fetch_metadata_with(&fake, 1), Err(MetadataError::HttpStatus(503))));
        assert_eq!(fake.urls.lock().unwrap().len(), 2);
    }
}
