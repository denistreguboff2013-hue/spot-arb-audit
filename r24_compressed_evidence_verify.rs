use serde_json::json;
use spotarb_native_market_core::compressed_evidence::verify_compressed_evidence_dir;
fn main()->Result<(),Box<dyn std::error::Error>>{
    let path=std::env::args().nth(1).ok_or("usage: r24_compressed_evidence_verify DIR")?;
    let v=verify_compressed_evidence_dir(&path)?;
    let ok=v.bad_hash==0&&v.nonmonotonic==0&&v.internal_offer_seq_gaps==0&&v.retention_accounting_ok;
    println!("{}",serde_json::to_string(&json!({"r24_compressed_evidence_verify":ok,"path":path,"verify":v}))?);
    if !ok{std::process::exit(2)}
    Ok(())
}
