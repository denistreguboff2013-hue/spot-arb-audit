//! R40 bounded read-only live PAPER audit sanity. NEVER sends Telegram.
use spotarb_native_market_core::native_virtual_four_leg_provenance_review::inspect_model_trade_notices;
use serde_json::{Value,json};
use std::{fs,path::Path};
fn main(){
 let root=Path::new("/mnt/spotarb_db_test/astra_paper_continuous_v1");
 let c:Value=serde_json::from_slice(&fs::read(root.join("current_session.json")).expect("current")).expect("json");
 let session=c["session"].as_str().expect("session");
 assert!(session.starts_with("session_")&&session.len()<90&&
  session.bytes().all(|v|v.is_ascii_alphanumeric()||v==b'_'));
 let parent=root.join("sessions").join(session);
 let journal=parent.join("modeled_four_leg_journal.jsonl");
 let meta=fs::symlink_metadata(&journal).expect("journal");
 assert!(meta.is_file() && !meta.file_type().is_symlink());
 let (a,notices)=inspect_model_trade_notices(&journal).expect("verified_hash_chain");
 let s:Value=serde_json::from_slice(&fs::read(parent.join("status.json")).expect("status")).expect("status_json");
 let p=&s["paper_portfolio"]["counters"];
 println!("{}",json!({
 "schema":"R40_READ_ONLY_PROBE_V1",
 "verified_session":a.session,"journal_seq":a.sequence,
 "model_entries":a.entries,"model_closed":a.closed,
 "parsed_event_notices":notices.len(),
 "status_entries":p["entries"],"status_closed":p["closed"],
 "telegram_calls":0,"exchange_orders":0,"file_writes":0
 }));
}
