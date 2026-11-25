use spotarb_native_market_core::async_evidence::verify_evidence_dir;
use serde_json::json;
fn main()->Result<(),Box<dyn std::error::Error>>{
    let path=std::env::args().nth(1).ok_or("usage: r23_evidence_verify DIR")?;
    let v=verify_evidence_dir(&path)?;
    let ok=v.bad_hash==0&&v.nonmonotonic==0&&v.offer_seq_gaps==0;
    println!("{}",serde_json::to_string(&json!({"r23_evidence_verify":ok,"path":path,"verify":v}))?);
    if !ok{std::process::exit(2)}
    Ok(())
}
