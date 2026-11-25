//! Read-only integrity probe for PUBLIC-DEPTH-MODELED, never exchange truth.
use spotarb_native_market_core::native_virtual_four_leg_provenance_review::inspect_model_journal;
use std::{env,path::Path};
fn main(){
 let a:Vec<String>=env::args().collect();
 if a.len()!=3{
     eprintln!("usage: native_model_provenance_probe FILE SESSION");
     std::process::exit(64)
 }
 match inspect_model_journal(Path::new(&a[1])){
  Ok(state) if state.session==a[2] && state.sealed =>{
    println!("MODEL_JOURNAL_PASS session={} seq={} entries={} closed={} open={} sha256={} exchange_fill_proof=false real_trading=false",state.session,state.sequence,state.entries,state.closed,state.open,state.last_hash)
  }
  Ok(state)=>{
    eprintln!("MODEL_JOURNAL_DENIED wrong session or unsealed session={} sealed={}",state.session,state.sealed);
    std::process::exit(2)
  }
  Err(e)=>{
    eprintln!("MODEL_JOURNAL_DENIED {e:?}");std::process::exit(2)
  }
 }
}
