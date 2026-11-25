//! Thin wrapper around the same source-only path exposed by the actual entry point.
#[tokio::main(flavor="multi_thread",worker_threads=2)]
async fn main()->Result<(),String>{let root=std::env::args().nth(1).map(std::path::PathBuf::from).ok_or("output root")?;spotarb_native_market_core::ingress_source_only_r1205::run_source_only(root).await?;Ok(())}
