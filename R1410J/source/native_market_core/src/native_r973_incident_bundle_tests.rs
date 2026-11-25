// R973-R977: diagnostic state is never a resumption permission.
#[test] fn r973_interrupted_exact_same_hedge_exposes_only_census(){
 use crate::native_virtual_four_leg_provenance_review::ModeledFourLegJournal;
 let cp=location();let journal=cp.with_extension("r973.jsonl");
 let p=state();let started=now_ms()-5_000;
 ModeledFourLegJournal::start(&journal,"session_973",&p,0).unwrap();
 save_checkpoint(&cp,&p,1,crate::transport::monotonic_ns(),false).unwrap();
 let census=inspect_interrupted_modeled_bundle(
  &cp,&journal,"session_973",p.day_yek,started).unwrap();
 assert_eq!(census.checkpoint_revision,1);
 assert_eq!(census.observed_entries,1);
 assert_eq!(census.observed_closed,0);
 assert_eq!(census.open_bases,vec!["ONE"]);
 assert_eq!(census.modeled_phase_by_base["ONE"],"FUTURES_SHORT_MODELED");
 assert_eq!(census.automatic_resume_allowed,false);
 assert_eq!(census.real_orders_allowed,false);
 assert!(load_planned_handoff(&cp,crate::transport::monotonic_ns(),1).is_err());
 fs::remove_file(cp).unwrap();fs::remove_file(journal).unwrap();
}
#[test] fn r974_self_sha_valid_forged_quantity_cannot_be_census(){
 use crate::native_virtual_four_leg_provenance_review::ModeledFourLegJournal;
 let cp=location();let journal=cp.with_extension("r974.jsonl");
 let before=state();let started=now_ms()-5_000;
 ModeledFourLegJournal::start(&journal,"session_974",&before,0).unwrap();
 let mut modified=before.clone();
 if let PaperPhase::Hedged(ref mut pos)=modified.positions.get_mut("ONE").unwrap().phase{
  pos.spot_paid_quote+=5.0;
 }
 save_checkpoint(&cp,&modified,1,crate::transport::monotonic_ns(),false).unwrap();
 assert!(inspect_interrupted_modeled_bundle(
  &cp,&journal,"session_974",before.day_yek,started).is_err());
 fs::remove_file(cp).unwrap();fs::remove_file(journal).unwrap();
}
#[test] fn r975_sealed_planned_state_is_not_an_interrupted_census(){
 use crate::native_virtual_four_leg_provenance_review::ModeledFourLegJournal;
 let cp=location();let journal=cp.with_extension("r975.jsonl");
 let p=state();let started=now_ms()-5_000;
 let mut j=ModeledFourLegJournal::start(&journal,"session_975",&p,0).unwrap();
 j.seal(&p).unwrap();
 save_checkpoint(&cp,&p,1,crate::transport::monotonic_ns(),true).unwrap();
 inspect_checkpoint_model_alignment(&cp,&journal,"session_975",true).unwrap();
 assert!(inspect_interrupted_modeled_bundle(
  &cp,&journal,"session_975",p.day_yek,started).is_err());
 fs::remove_file(cp).unwrap();fs::remove_file(journal).unwrap();
}
#[test] fn r976_incident_requires_correct_journal_session_and_day(){
 use crate::native_virtual_four_leg_provenance_review::ModeledFourLegJournal;
 let cp=location();let journal=cp.with_extension("r976.jsonl");
 let p=state();let started=now_ms()-5_000;
 ModeledFourLegJournal::start(&journal,"session_976",&p,0).unwrap();
 save_checkpoint(&cp,&p,1,crate::transport::monotonic_ns(),false).unwrap();
 assert!(inspect_interrupted_modeled_bundle(
  &cp,&journal,"session_other",p.day_yek,started).is_err());
 assert!(inspect_interrupted_modeled_bundle(
  &cp,&journal,"session_976",p.day_yek+1,started).is_err());
 assert!(inspect_interrupted_modeled_bundle(
  &cp,&journal,"session_976",p.day_yek,now_ms()+100_000).is_err());
 fs::remove_file(cp).unwrap();fs::remove_file(journal).unwrap();
}
#[test] fn r977_torn_virtual_cover_must_quarantine_until_two_files_agree(){
 use crate::native_virtual_four_leg_provenance_review::ModeledFourLegJournal;
 let cp=location();let journal=cp.with_extension("r977.jsonl");
 let before=state();let started=now_ms()-5_000;
 let mut writer=ModeledFourLegJournal::start(&journal,"session_977",&before,0).unwrap();
 save_checkpoint(&cp,&before,1,crate::transport::monotonic_ns(),false).unwrap();
 let mut after=before.clone();
 let position=match &before.positions["ONE"].phase{
  PaperPhase::Hedged(p)=>p.clone(),_=>panic!("hedged expected")
 };
 after.positions.get_mut("ONE").unwrap().phase=PaperPhase::AwaitingSpotExit{
  pos:position,cover_quote:10.01,
  cover_ns:crate::transport::monotonic_ns(),close_estimate_quote:0.1
 };
 after.stats.futures_covers+=1;
 writer.record_change("ONE",&before,&after).unwrap();
 assert!(inspect_interrupted_modeled_bundle(
  &cp,&journal,"session_977",before.day_yek,started).is_err());
 save_checkpoint(&cp,&after,2,crate::transport::monotonic_ns(),false).unwrap();
 let census=inspect_interrupted_modeled_bundle(
  &cp,&journal,"session_977",before.day_yek,started).unwrap();
 assert_eq!(census.modeled_phase_by_base["ONE"],"FUTURES_COVER_MODELED");
 assert!(!census.automatic_resume_allowed);
 fs::remove_file(cp).unwrap();fs::remove_file(journal).unwrap();
}

#[test] fn r978_one_nanodollar_forgery_denied_even_with_valid_checkpoint_sha(){
 use crate::native_virtual_four_leg_provenance_review::ModeledFourLegJournal;
 let cp=location();let journal=cp.with_extension("r978.jsonl");
 let before=state();let started=now_ms()-5_000;
 ModeledFourLegJournal::start(&journal,"session_978",&before,0).unwrap();
 save_checkpoint(&cp,&before,1,crate::transport::monotonic_ns(),false).unwrap();
 assert!(inspect_interrupted_modeled_bundle(
  &cp,&journal,"session_978",before.day_yek,started).is_ok());
 let mut forged=before.clone();
 forged.stats.modeled_realized_usdt+=0.000000001;
 save_checkpoint(&cp,&forged,2,crate::transport::monotonic_ns(),false).unwrap();
 assert!(inspect_checkpoint_model_alignment(
  &cp,&journal,"session_978",false).is_err());
 assert!(inspect_interrupted_modeled_bundle(
  &cp,&journal,"session_978",before.day_yek,started).is_err());
 fs::remove_file(cp).unwrap();fs::remove_file(journal).unwrap();
}
