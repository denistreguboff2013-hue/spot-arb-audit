//! Astra R151: synthetic, isolated, cross-process SQLite mirror smoke.
use spotarb_native_market_core::native_isolated_sqlite_mirror_r147_review::SqliteMirrorReview;
use spotarb_native_market_core::native_mirror_outbox_protocol_r139_review::MirrorOutboxRowR139;
use std::{env,path::PathBuf,time::{SystemTime,UNIX_EPOCH}};
fn run()->Result<(),String>{
 let a:Vec<String>=env::args().collect();
 if a.len()<3||a.len()>5{return Err("usage: MODE ROOT [SEQ PREV]".into())}
 let root=PathBuf::from(&a[2]);
 let now=SystemTime::now().duration_since(UNIX_EPOCH)
  .map_err(|_|"clock")?.as_millis() as u64;
 let parse=|idx:usize|->Result<u64,String>{
  a.get(idx).ok_or("missing index".to_string())?.parse().map_err(|_|"bad index".into())
 };
 let db=if a[1]=="init"{
  if a.len()!=3{return Err("init takes only ROOT".into())}
  SqliteMirrorReview::initialize(&root)
 }else{SqliteMirrorReview::load(&root)}.map_err(|e|format!("{e:?}"))?;
 match a[1].as_str(){
  "init"=>println!("R151_INIT_OK"),
  "source"=>{
   if a.len()!=5{return Err("source requires SEQ PREV".into())}
   let mut row:MirrorOutboxRowR139=serde_json::from_str(include_str!(
    "../../config/native_mirror_outbox_python_fixture_r141_review.json"))
    .map_err(|_|"fixture")?;
   row.seq=parse(3)?;row.prev_seq=parse(4)?;
   row.tx_id=format!("tx-{:032x}",row.seq);
   db.append_synthetic_source(row,now).map_err(|e|format!("{e:?}"))?;
   println!("R151_SOURCE_OK")
  },
  "crash"=>{
   if a.len()!=5{return Err("crash requires SEQ PHASE".into())}
   let v=db.apply_crash_r153(parse(3)?,now,&a[4])
    .map_err(|e|format!("{e:?}"))?;
   println!("R153_EXIT seq={}",v.seq);
  },
  "apply"=>{
   if a.len()!=4{return Err("apply requires SEQ".into())}
   let v=db.apply_offline(parse(3)?,now,false).map_err(|e|format!("{e:?}"))?;
   println!("R151_APPLY_OK seq={} rows={} production_db2={}",
     v.seq,v.rows,v.production_db2)
  },
  "status"=>{
   if a.len()!=3{return Err("status takes ROOT only".into())}
   let (seq,rows)=db.review_integrity_r152(now).map_err(|e|format!("{e:?}"))?;
   println!("R151_STATUS seq={seq} rows={rows} production_db2=false")
  },
  _=>return Err("unsupported mode".into())
 }
 Ok(())
}
fn main(){
 if let Err(e)=run(){eprintln!("R151_DENY {e}");std::process::exit(2)}
}
