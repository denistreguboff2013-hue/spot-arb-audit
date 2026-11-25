use spotarb_native_market_core::native_paper_continuity_checkpoint_review::inspect_planned_seal;
use std::{env,path::Path};
fn main(){
 let a:Vec<String>=env::args().collect();
 if a.len()!=4{eprintln!("PATH DAY ENTRIES");std::process::exit(64)}
 let day:i64=a[2].parse().unwrap();
 let n:u64=a[3].parse().unwrap();
 match inspect_planned_seal(Path::new(&a[1]),day,n){
  Ok(())=>println!("SEAL_PASS public-paper-only"),
  Err(e)=>{println!("SEAL_DENIED {e:?}");std::process::exit(2)}
 }
}
