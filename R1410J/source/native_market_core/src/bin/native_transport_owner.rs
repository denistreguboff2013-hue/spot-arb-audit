//! Review-only owner process; public market data and loopback control fixtures.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).ok_or("usage: native_transport_owner <isolated-config.json>")?;
    spotarb_native_market_core::transport::run(&path)
}
