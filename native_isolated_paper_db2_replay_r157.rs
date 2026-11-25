//! Astra R157: one-shot read-only modeled PAPER snapshot to synthetic private DBs.
//! NO real trading, NO private exchange API, NO writes to PAPER or legacy DBs.
use spotarb_native_market_core::native_isolated_sqlite_mirror_r147_review::{
 SqliteMirrorReview,project_verified_paper_journal_r156,
};
use std::{env,path::Path,time::{SystemTime,UNIX_EPOCH}};
fn run()->Result<(),String>{
 let a:Vec<String>=env::args().collect();
 if a.len()<3||a.len()>5{return Err("usage: inspect JOURNAL | replay JOURNAL ROOT | crashbatch JOURNAL ROOT PHASE".into())}
 let now=SystemTime::now().duration_since(UNIX_EPOCH)
  .map_err(|_|"clock")?.as_millis() as u64;
 let snapshot=project_verified_paper_journal_r156(Path::new(&a[2]),now)
  .map_err(|e|format!("journal {e:?}"))?;
 match a[1].as_str(){
  "inspect"=>{
   if a.len()!=3{return Err("inspect JOURNAL only".into())}
   println!("R156_MODEL_READ_OK source_seq={} events={} paper_only=true may_trade=false",
      snapshot.original_seq,snapshot.rows.len());
  },
  "replay"|"crashbatch"=>{
   if a[1]=="replay" && !(a.len()==4||a.len()==5) || a[1]=="crashbatch" && a.len()!=5{
    return Err("wrong argument count".into())
   }
   let root=Path::new(&a[3]);
   if root.parent()!=Some(Path::new("/tmp"))||
     !root.file_name().and_then(|s|s.to_str()).unwrap_or("").starts_with("astra_r147_isolated_")||
     !std::fs::symlink_metadata(root).map_err(|_|"root missing")?
       .file_type().is_dir(){return Err("isolated /tmp root required".into())}
   let db=match SqliteMirrorReview::load(root){
    Ok(v)=>v,
    Err(_)=>SqliteMirrorReview::initialize(root)
     .map_err(|e|format!("isolated setup {e:?}"))?,
   };
   if a[1]=="crashbatch"{
    if snapshot.rows.len()<8{return Err("need 8 modeled events".into())}
    let phase=a[4].as_str();
    if !matches!(phase,"after_first_insert"|"after_cas"|"after_commit"){
     return Err("unsupported fault phase".into())
    }
    for row in &snapshot.rows{
     db.append_synthetic_source(row.clone(),now)
      .map_err(|e|format!("seed {e:?}"))?;
    }
    let _=db.apply_batch_with_fault_r161(&snapshot.rows[..8],now,Some(phase))
      .map_err(|e|format!("batch {e:?}"))?;
    return Err("fault phase did not SIGKILL".into())
   }
   let batch_size=if a.len()==5{a[4].parse::<usize>()
     .map_err(|_|"batch size must be integer")?}else{8};
   let report=db.replay_modeled_paper_r157(&snapshot,now,batch_size)
     .map_err(|e|format!("replay {e:?}"))?;
   println!("R157_ISOLATED_PASS examined={} applied={} already={} final_seq={} production_db2={} may_trade={}",
     report.examined,report.newly_applied,report.already_applied,
     report.final_seq,report.production_db2,report.may_trade);
  },
  _=>return Err("unknown mode".into()),
 }
 Ok(())
}
fn main(){
 if let Err(e)=run(){eprintln!("R157_DENY {e}");std::process::exit(2)}
}
