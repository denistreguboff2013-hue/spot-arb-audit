// Astra P0-01 R968. Cold, read-only original PAPER four-leg proof.
// Replays full last modeled PAPER PHASE payloads, not just their names.
// The journal has its own local sha chain but NO independent signer;
// successful diagnostic NEVER authorizes automatic crash restoration,
// REAL orders, exchange fills, or a rollback of existing runtime state.
pub fn inspect_modeled_exact_open_phase_payloads(
 path:&std::path::Path
)->Result<std::collections::BTreeMap<String,serde_json::Value>,AuditError>{
 let validated=read_all(path)?;
 let bytes=std::fs::read(path)?;
 if bytes.is_empty()||bytes.len() as u64>MAX_FILE||bytes.last()!=Some(&b'\n'){
  return Err(deny("partial or oversized modeled phase journal"))
 }
 let mut phases=std::collections::BTreeMap::<String,serde_json::Value>::new();
 let mut saw_genesis=false;
 let mut seq=1u64;
 let mut previous=ZERO.to_string();
 for line in bytes.split(|byte|*byte==b'\n').filter(|line|!line.is_empty()){
  let entry:Line=serde_json::from_slice(line)
    .map_err(|_|deny("malformed modeled phase row"))?;
  if entry.seq!=seq||entry.previous_hash!=previous
    ||entry.checksum!=hash(entry.seq,&previous,&entry.body){
    return Err(deny("phase journal changed after canonical verification"))
  }
  previous=entry.checksum.clone();
  seq=seq.checked_add(1).ok_or(deny("modeled phase sequence overflow"))?;
  let payload:ModelAuditPayload=serde_json::from_str(&entry.body)
    .map_err(|_|deny("malformed modeled phase record"))?;
  match payload.stage.as_str(){
   "SESSION_START"=>{
    if saw_genesis{return Err(deny("multiple modeled session starts"))}
    saw_genesis=true;
    let images=payload.details["initial_virtual_positions"].as_object()
      .ok_or(deny("missing original carried portfolio images"))?;
    for (base,machine) in images{
     let img=machine.get("phase").ok_or(deny("missing initial phase payload"))?;
     if phases.insert(base.to_owned(),img.clone()).is_some(){
      return Err(deny("repeated initial modeled base"))
     }
    }
   },
   "SPOT_BUY_MODELED"|"FUTURES_SHORT_MODELED"|"FUTURES_COVER_MODELED"=>{
    if !saw_genesis{return Err(deny("no modeled genesis"))}
    let after=payload.details.get("next_virtual_phase")
      .filter(|v|!v.is_null()).ok_or(deny("missing modeled after-image"))?;
    phases.insert(payload.base.clone(),after.clone());
   },
   "SPOT_SELL_MODELED"=>{
    if payload.details.get("next_virtual_phase")
      .is_some_and(|value|!value.is_null())||phases.remove(&payload.base).is_none(){
      return Err(deny("invalid modeled terminal after-image"))
    }
   },
   "SESSION_SEAL"=>{
    let images=payload.details["virtual_positions"].as_object()
      .ok_or(deny("missing modeled seal payloads"))?;
    if images.len()!=phases.len(){return Err(deny("modeled seal position count drift"))}
    for (base,machine) in images{
     if phases.get(base)!=machine.get("phase"){
      return Err(deny("modeled seal position payload drift"))
     }
    }
   },
   "STRATEGY_VIEW_MODELED_V1"=>{
    let images=payload.details["strategy_image"]["portfolio"]["positions"].as_object()
      .ok_or(deny("missing modeled strategy position payloads"))?;
    if images.len()!=phases.len(){return Err(deny("modeled strategy open-count drift"))}
    for (base,machine) in images{
     if phases.get(base)!=machine.get("phase"){
      return Err(deny("modeled strategy position payload drift"))
     }
    }
   },
   _=>return Err(deny("unrecognized modeled phase record")),
  }
 }
 if seq.checked_sub(1)!=Some(validated.sequence)
  ||previous!=validated.last_hash{
  return Err(deny("phase journal changed between canonical read and phase proof"))
 }
 if !saw_genesis||phases.len()!=validated.open
  ||phases.keys().collect::<Vec<_>>()!=validated.model_phase_by_base.keys().collect::<Vec<_>>(){
  return Err(deny("full modeled position set differs from validated phases"))
 }
 Ok(phases)
}

#[cfg(test)]
mod r968_cold_bundle_tests{
 use super::*;
 use crate::native_paper_continuity_checkpoint_review::{
  save_checkpoint,inspect_checkpoint_model_alignment,load_planned_handoff
 };
 use crate::native_paper_portfolio_review::PaperPortfolio;
 use crate::native_paper_market_execution_review::{
  PaperAssumptions,PaperMachine,PaperPosition,PaperPhase,
 };
 use std::sync::atomic::{AtomicU64,Ordering};
 static ID:AtomicU64=AtomicU64::new(1);
 struct Fixture{dir:std::path::PathBuf}
 impl Fixture{
  fn new()->Self{
   let mut dir=std::env::temp_dir();
   dir.push(format!("astra_r968_cold_bundle_{}_{}",
      std::process::id(),ID.fetch_add(1,Ordering::Relaxed)));
   std::fs::create_dir(&dir).unwrap();Self{dir}
  }
  fn check(&self)->std::path::PathBuf{self.dir.join("checkpoint.json")}
  fn journal(&self)->std::path::PathBuf{self.dir.join("modeled.jsonl")}
 }
 impl Drop for Fixture{fn drop(&mut self){let _=std::fs::remove_dir_all(&self.dir);}}
 fn seeded()->PaperPortfolio{
  let now=std::time::SystemTime::now()
   .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
  let mut p=PaperPortfolio::new(now).unwrap();
  p.stats.entries=1;p.stats.futures_hedges=1;
  let mono=crate::transport::monotonic_ns()-10_000_000;
  let pos=PaperPosition{
   base:"ONE".into(),signal_ns:mono,spot_buy_ns:mono+1,
   signal_estimated_net_bps:60.0,spot_gross_base:1.0,
   spot_delivered_base:1.0,spot_paid_quote:10.0,
   futures_short_ns:Some(mono+2),futures_short_proceeds_quote:Some(10.2),
   futures_short_base:Some(1.0),hedge_entry_estimated_net_bps:Some(60.0)
  };
  let mut machine=PaperMachine::new(PaperAssumptions::default()).unwrap();
  machine.phase=PaperPhase::Hedged(pos);
  p.positions.insert("ONE".into(),machine);
  p
 }
 #[test]fn r968_same_stage_forged_amount_with_valid_checkpoint_sha_denied(){
  let fixture=Fixture::new();let valid=seeded();
  ModeledFourLegJournal::start(&fixture.journal(),"session_968",&valid,0).unwrap();
  save_checkpoint(&fixture.check(),&valid,1,crate::transport::monotonic_ns(),false).unwrap();
  inspect_checkpoint_model_alignment(&fixture.check(),&fixture.journal(),
    "session_968",false).unwrap();
  let mut forged=valid.clone();
  if let PaperPhase::Hedged(ref mut pos)=forged.positions.get_mut("ONE").unwrap().phase{
   pos.spot_paid_quote=11.25;
  }
  save_checkpoint(&fixture.check(),&forged,2,crate::transport::monotonic_ns(),false).unwrap();
  assert_eq!(forged.stats.entries,valid.stats.entries);
  assert_eq!(forged.stats.closed,valid.stats.closed);
  assert_eq!(forged.positions.len(),valid.positions.len());
  assert!(inspect_checkpoint_model_alignment(&fixture.check(),&fixture.journal(),
    "session_968",false).is_err(),"same four-leg stage, changed amount must fail");
  // The local hash is valid but the episode is NOT completed: no
  // automatic crash transfer even if the first alignment was true.
  assert!(load_planned_handoff(&fixture.check(),
    crate::transport::monotonic_ns(),1).is_err());
 }
 #[test]fn r969_same_stage_forged_hedge_quantity_and_timestamp_denied(){
  let fixture=Fixture::new();let valid=seeded();
  ModeledFourLegJournal::start(&fixture.journal(),"session_969",&valid,0).unwrap();
  let mut forged=valid.clone();
  if let PaperPhase::Hedged(ref mut pos)=forged.positions.get_mut("ONE").unwrap().phase{
   pos.futures_short_base=Some(0.75);
   pos.spot_buy_ns+=1;
  }
  save_checkpoint(&fixture.check(),&forged,1,crate::transport::monotonic_ns(),false).unwrap();
  assert!(inspect_checkpoint_model_alignment(&fixture.check(),&fixture.journal(),
    "session_969",false).is_err());
 }
 #[test]fn r970_legitimate_full_hedge_to_cover_stage_then_exact_alignment(){
  let fixture=Fixture::new();let original=seeded();
  let mut journal=ModeledFourLegJournal::start(
    &fixture.journal(),"session_970",&original,0).unwrap();
  let mut next=original.clone();
  let pos=match &original.positions["ONE"].phase{
   PaperPhase::Hedged(p)=>p.clone(),_=>panic!("must be hedged")
  };
  next.positions.get_mut("ONE").unwrap().phase=PaperPhase::AwaitingSpotExit{
   pos,cover_quote:10.01,cover_ns:crate::transport::monotonic_ns(),
   close_estimate_quote:0.1
  };
  next.stats.futures_covers=1;
  save_checkpoint(&fixture.check(),&original,1,
    crate::transport::monotonic_ns(),false).unwrap();
  assert!(inspect_checkpoint_model_alignment(&fixture.check(),&fixture.journal(),
    "session_970",false).is_ok());
  journal.record_change("ONE",&original,&next).unwrap();
  assert!(inspect_checkpoint_model_alignment(&fixture.check(),&fixture.journal(),
    "session_970",false).is_err(),"journal-ahead version must quarantine");
  save_checkpoint(&fixture.check(),&next,2,crate::transport::monotonic_ns(),false).unwrap();
  inspect_checkpoint_model_alignment(&fixture.check(),&fixture.journal(),
    "session_970",false).unwrap();
  let mut forged=next.clone();
  if let PaperPhase::AwaitingSpotExit{ref mut cover_quote,..}=
    forged.positions.get_mut("ONE").unwrap().phase{*cover_quote=10.02}
  save_checkpoint(&fixture.check(),&forged,3,crate::transport::monotonic_ns(),false).unwrap();
  assert!(inspect_checkpoint_model_alignment(&fixture.check(),&fixture.journal(),
    "session_970",false).is_err(),"same stage forged cover quote denied");
 }
 #[test]fn r971_cut_or_mutated_journal_refuses_full_payload_attestation(){
  let fixture=Fixture::new();let original=seeded();
  ModeledFourLegJournal::start(&fixture.journal(),"session_971",&original,0).unwrap();
  let path=fixture.journal();
  let verified=std::fs::read(&path).unwrap();
  assert!(inspect_modeled_exact_open_phase_payloads(&path).is_ok());
  std::fs::write(&path,&verified[..verified.len()-1]).unwrap();
  assert!(inspect_modeled_exact_open_phase_payloads(&path).is_err());
  let mut changed=verified.clone();
  let at=changed.iter().position(|b|*b==b'0').unwrap();
  changed[at]=b'1';
  std::fs::write(&path,changed).unwrap();
  assert!(inspect_modeled_exact_open_phase_payloads(&path).is_err());
 }
}
