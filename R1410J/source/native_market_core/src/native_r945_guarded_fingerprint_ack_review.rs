// R945 Astra P0-01 PRIVATE research: verify fully canonically under WAL
// snapshot, then compare SHA256 of the EXACT sqlite json_object outbox
// row bytes under a short source write lock. New concurrent append tails
// are still fully canonically verified under that lock.
// Older rows must match each source snapshot row, including outbox
// metadata; mutations/reordering/gaps abort. No skip of new tails.
fn r945_select_outbox()->Result<String,EventOutboxR366Error>{
 Ok(r805_window_sql(R726_OUTBOX,"ORDER BY seq")?
   .replacen("SELECT ","SELECT seq,payload_bytes,",1))
}
fn r945_capture_outbox_fingerprints(src:&Db,h:&R805Head)
 ->Result<Vec<(u64,String)>,EventOutboxR366Error>{
 if h.retained>64{return Err(EventOutboxR366Error::Pressure)}
 let mut q=Stmt::new(src,&r945_select_outbox()?).map_err(r366_error)?;
 let mut result=Vec::new();
 let mut previous=h.cleaned;
 while q.step().map_err(r366_error)?==SQLITE_ROW{
  let seq=q.num(0).map_err(r366_error)?;
  if seq!=previous.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?{
   return Err(EventOutboxR366Error::Corrupt)
  }
  let raw=q.strval(2).map_err(r366_error)?;
  result.push((seq,r366_sha(&raw)));
  if result.len()>64{return Err(EventOutboxR366Error::Pressure)}
  previous=seq;
 }
 if result.len()!=h.retained as usize||previous!=h.latest{
  return Err(EventOutboxR366Error::Corrupt)
 }
 Ok(result)
}
fn r945_verify_fingerprints_and_canonical_new_tail(
 src:&Db,now:&R805Head,generation:&str,old:&[(u64,String)]
)->Result<(),EventOutboxR366Error>{
 if now.retained>64{return Err(EventOutboxR366Error::Pressure)}
 let mut q=Stmt::new(src,&r945_select_outbox()?).map_err(r366_error)?;
 let mut count=0usize;
 let mut previous=now.cleaned;
 let mut bytes=0u64;
 loop {
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let seq=q.num(0).map_err(r366_error)?;
    let row_bytes=q.num(1).map_err(r366_error)?;
    let raw=q.strval(2).map_err(r366_error)?;
    if seq!=previous.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?{
     return Err(EventOutboxR366Error::Corrupt)
    }
    if count<old.len(){
     if old[count].0!=seq||old[count].1!=r366_sha(&raw){
      return Err(EventOutboxR366Error::Corrupt)
     }
    }else{
     let row:serde_json::Value=serde_json::from_str(&raw)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
     let (validated_seq,validated_bytes)=r805_validate_row(
      &row,previous,now.latest,generation)?;
     if validated_seq!=seq||validated_bytes!=row_bytes{
      return Err(EventOutboxR366Error::Corrupt)
     }
    }
    bytes=bytes.checked_add(row_bytes).ok_or(EventOutboxR366Error::Pressure)?;
    previous=seq;
    count+=1;
    if count>64{return Err(EventOutboxR366Error::Pressure)}
   },
   _=>return Err(EventOutboxR366Error::Corrupt),
  }
 }
 if count!=now.retained as usize||count<old.len()
   ||previous!=now.latest{
  return Err(EventOutboxR366Error::Corrupt)
 }
 // The original r805_read_head independently checked row sum vs
 // mirror_control. The total is checked AGAIN here as a guard.
 let ctl=r366_query_num(src,"SELECT retained_bytes FROM mirror_control WHERE id=1")?;
 if bytes!=ctl{return Err(EventOutboxR366Error::Corrupt)}
 Ok(())
}
pub(super) fn r945_private_fast_fingerprint_ack(
 root:&Path,src:&Db,dst:&Db,v:&serde_json::Value,run:&str
)->Result<(u64,u64),EventOutboxR366Error>{
 r932_guarded_incremental_prefix_ack_impl(root,src,dst,v,run,true)
}
#[test] fn r945_capture_and_compare_identical_retained_chain(){
 let (root,h,_)=pair(3);let v=r586_oracle();
 r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
 let (hd,c,_)=r805_read_head(&root.0,&h.source,&h.sink).unwrap();
 r805_verify_chain(&h.source,&hd,r726_s(&c,"feed_generation").unwrap()).unwrap();
 let old=r945_capture_outbox_fingerprints(&h.source,&hd).unwrap();
 assert_eq!(old.len(),3);
 assert_eq!(r945_verify_fingerprints_and_canonical_new_tail(
  &h.source,&hd,r726_s(&c,"feed_generation").unwrap(),&old),Ok(()));
}
#[test] fn r946_poison_previously_verified_future_row_refuses_ack(){
 let (root,h,_)=r937_ready_one();let v=r586_oracle();
 let (hd,c,_)=r805_read_head(&root.0,&h.source,&h.sink).unwrap();
 r805_verify_chain(&h.source,&hd,r726_s(&c,"feed_generation").unwrap()).unwrap();
 let old=r945_capture_outbox_fingerprints(&h.source,&hd).unwrap();
 exec(&h.source,"UPDATE mirror_outbox SET payload_hash='tampered' WHERE seq=1").unwrap();
 assert_eq!(r945_verify_fingerprints_and_canonical_new_tail(
  &h.source,&hd,r726_s(&c,"feed_generation").unwrap(),&old),
  Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r945_private_fast_fingerprint_ack(&root.0,&h.source,&h.sink,
  &v,"r937_probe"),Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
#[test] fn r947_guarded_fast_ack_exact_64_batches_and_final_parity(){
 let root=Root::new();let v=r586_oracle();
 let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 r924_install_private_paper_only_guards(&src,&dst,&v).unwrap();
 r932_certify_private_empty_genesis(&root.0,&src,&dst,&v,"r947_probe").unwrap();
 let mut h=R819OriginalPaperHistory::new(&root.0,"r947_probe").unwrap();
 let mut owner=Owner::new(0,1,800).unwrap();
 for i in (1..=2048).step_by(32){
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,
   &(i..i+32).map(event).collect::<Vec<_>>()),Ok(((i+31)/32,32)));
  assert_eq!(r931_guarded_verified_burst(&root.0,&src,&dst,&v,1),Ok(1));
  assert_eq!(r945_private_fast_fingerprint_ack(&root.0,&src,&dst,&v,
   "r947_probe"),Ok((1,0)));
 }
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(2048));
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
}

#[test] fn r949_legitimate_new_tail_is_fully_validated_after_old_snapshot(){
 let (root,mut h,mut owner)=pair(3);let v=r586_oracle();
 r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
 let (before,control,_)=r805_read_head(&root.0,&h.source,&h.sink).unwrap();
 r805_verify_chain(&h.source,&before,r726_s(&control,"feed_generation").unwrap()).unwrap();
 let old=r945_capture_outbox_fingerprints(&h.source,&before).unwrap();
 assert_eq!(old.len(),3);
 assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,
  &(97..=128).map(event).collect::<Vec<_>>()),Ok((4,32)));
 let (now,c,_)=r805_read_head(&root.0,&h.source,&h.sink).unwrap();
 assert_eq!(r945_verify_fingerprints_and_canonical_new_tail(
  &h.source,&now,r726_s(&c,"feed_generation").unwrap(),&old),Ok(()));
 assert_eq!(now.latest,4);
}
#[test] fn r950_corrupt_new_tail_cannot_bypass_fingerprint_recheck(){
 let (root,mut h,mut owner)=pair(3);let v=r586_oracle();
 r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
 let (before,control,_)=r805_read_head(&root.0,&h.source,&h.sink).unwrap();
 r805_verify_chain(&h.source,&before,r726_s(&control,"feed_generation").unwrap()).unwrap();
 let old=r945_capture_outbox_fingerprints(&h.source,&before).unwrap();
 assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,
  &(97..=128).map(event).collect::<Vec<_>>()),Ok((4,32)));
 exec(&h.source,"UPDATE mirror_outbox SET payload_hash='poison_new_tail' WHERE seq=4").unwrap();
 let (now,c,_)=r805_read_head(&root.0,&h.source,&h.sink).unwrap();
 assert_eq!(r945_verify_fingerprints_and_canonical_new_tail(
  &h.source,&now,r726_s(&c,"feed_generation").unwrap(),&old),
  Err(EventOutboxR366Error::Corrupt));
}
