// R924 Astra P0-01: PRIVATE ONLY "PAPER insert-only" schema extension.
// Existing 17 Python-table data/columns retained; extra SQLite triggers
// prohibit UPDATE/DELETE of all original rows and INSERT to all 16
// other tables. Only events INSERT is allowed. Do not deploy/migrate
// historic DB1 or claim compatibility with arbitrary Python 17-table ops.
fn r924_guard_statements(v:&serde_json::Value)
 ->Result<Vec<(String,String)>,EventOutboxR366Error>{
 let tables=v["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?;
 if tables.len()!=17{return Err(EventOutboxR366Error::Schema)}
 let mut all=Vec::new();
 for entry in tables{
  let name=entry["table"].as_str().ok_or(EventOutboxR366Error::Schema)?;
  let ident=r586_identifier(name)?;
  for action in ["UPDATE","DELETE","INSERT"]{
   if action=="INSERT"&&name=="events"{continue}
   let trigger=format!("r924_guard_{}_{}",name,action.to_ascii_lowercase());
   let identifier=r586_identifier(&trigger)?;
   let ddl=format!("CREATE TRIGGER {identifier} BEFORE {action} ON {ident} BEGIN SELECT RAISE(ABORT,'r924 original PAPER-only mutation denied'); END");
   all.push((trigger,ddl));
  }
 }
 if all.len()!=50{return Err(EventOutboxR366Error::Schema)}
 Ok(all)
}
pub(super) fn r924_verify_installed_guards(db:&Db,manifest:&serde_json::Value)
 ->Result<(),EventOutboxR366Error>{
 let guards=r924_guard_statements(manifest)?;
 let mut q=Stmt::new(db,"SELECT name,sql FROM sqlite_master
   WHERE type='trigger' AND name LIKE 'r924_guard_%' ORDER BY name")
   .map_err(r366_error)?;
 let mut actual=std::collections::BTreeMap::new();
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let key=q.strval(0).map_err(r366_error)?;
    let val=q.strval(1).map_err(r366_error)?;
    if actual.insert(key,val).is_some(){return Err(EventOutboxR366Error::Corrupt)}
   },
   _=>return Err(EventOutboxR366Error::Corrupt),
  }
 }
 drop(q);
 if actual.len()!=guards.len(){return Err(EventOutboxR366Error::Corrupt)}
 for (name,ddl) in guards{
  if actual.get(&name)!=Some(&ddl){return Err(EventOutboxR366Error::Corrupt)}
 }
 Ok(())
}
pub(super) fn r924_install_private_paper_only_guards(
 src:&Db,dst:&Db,manifest:&serde_json::Value
)->Result<(),EventOutboxR366Error>{
 let baseline=r756_schema_hash(src)?;
 if baseline!=r756_schema_hash(dst)?{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let txs=Tx::begin(src).map_err(r366_error)?;
 let txd=Tx::begin(dst).map_err(r366_error)?;
 for (_,ddl) in r924_guard_statements(manifest)?{
  exec(src,&ddl).map_err(r366_error)?;
  exec(dst,&ddl).map_err(r366_error)?;
 }
 let new_hash=r756_schema_hash(src)?;
 if new_hash==baseline||new_hash!=r756_schema_hash(dst)?{
  return Err(EventOutboxR366Error::Corrupt)
 }
 r924_verify_installed_guards(src,manifest)?;
 r924_verify_installed_guards(dst,manifest)?;
 let new_version=r366_query_num(src,"PRAGMA schema_version")?;
 exec(dst,&format!("UPDATE mirror_meta SET schema_hash={},
  schema_version={new_version} WHERE id=1",
  r586_sql_literal(&serde_json::json!(new_hash))?)).map_err(r366_error)?;
 txd.commit().map_err(r366_error)?;
 txs.commit().map_err(r366_error)?;
 Ok(())
}

// R925: no blanket PRAGMA data_version check; after a fully proved WAL
// snapshot, accept ONLY writer appends while 17 physical collector tables
// are SQL-trigger immutable (except events INSERT). Still revalidate
// complete retained outbox chain under DB1 short write lock to detect
// unsealed outbox changes. Revalidate final exact guard DDL, source
// receipt, counters, identity/schema/DB2 hash before deleting prefix.
// This is not a proof against a privileged hostile SQLite file owner who
// can defeat SQL triggers, forge provenance or reset PRAGMA schema_version.
pub(super) fn r925_guarded_paper_prefix_ack<F>(
 root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,run:&str,
 after_snapshot:F
)->Result<(u64,u64),EventOutboxR366Error>
where F:FnOnce()->Result<(),EventOutboxR366Error>{
 r924_verify_installed_guards(src,manifest)?;
 r924_verify_installed_guards(dst,manifest)?;
 exec(src,"BEGIN DEFERRED").map_err(r366_error)?;
 let snapshot=R805SourceRead(src);
 let dest_tx=Tx::begin(dst).map_err(r366_error)?;
 let (h,c,m)=r805_read_head(root,src,dst)?;
 if h.applied<=h.cleaned{return Err(EventOutboxR366Error::Conflict)}
 if h.retained>64{return Err(EventOutboxR366Error::Pressure)}
 let generation=r726_s(&c,"feed_generation")?;
 let (acked,bytes,rows,hash)=
  r907_verify_retained_and_extract_prefix(src,&h,generation,run)?;
 if m["applied_hash"].as_str()!=Some(hash.as_str()){
  return Err(EventOutboxR366Error::Corrupt)
 }
 if r366_query_num(dst,"SELECT COUNT(*) FROM pragma_foreign_key_check")?!=0{
  return Err(EventOutboxR366Error::Corrupt)
 }
 r879_private_paper_prefix_parity(src,dst,manifest,run,h.applied)?;
 r895_verify_new_paper_rows(src,dst,manifest,&rows)?;
 after_snapshot()?;
 drop(snapshot);
 let source_tx=Tx::begin(src).map_err(r366_error)?;
 r924_verify_installed_guards(src,manifest)?;
 r924_verify_installed_guards(dst,manifest)?;
 let (now,control,meta)=r805_read_head(root,src,dst)?;
 // Cleaned or destination receipt changing across validation is a race.
 // New source latest/retained is expected under continuous intake.
 if now.cleaned!=h.cleaned||now.applied!=h.applied
  ||now.latest<h.latest||now.retained<h.retained
  ||meta!=m||control["feed_generation"]!=c["feed_generation"]
  ||control["schema_generation"]!=c["schema_generation"]
  ||control["baseline_seq"]!=c["baseline_seq"]
  ||control["state"]!=c["state"]{
  return Err(EventOutboxR366Error::Conflict)
 }
 // Check every retained original envelope with same canonical source
 // operations/hashes; any tail corruption prevents deleting ANY prefix.
 r805_verify_chain(src,&now,r726_s(&control,"feed_generation")?)?;
 let mut seal=Stmt::new(src,&format!(
  "SELECT payload_hash FROM mirror_outbox WHERE seq={}",h.applied))
  .map_err(r366_error)?;
 if seal.step().map_err(r366_error)?!=SQLITE_ROW
  ||seal.strval(0).map_err(r366_error)?!=hash{
  return Err(EventOutboxR366Error::Corrupt)
 }
 drop(seal);
 let sql=format!("DELETE FROM mirror_outbox WHERE seq<={};
  UPDATE mirror_control SET cleanup_through={},
    retained_records=retained_records-{acked},
    retained_bytes=retained_bytes-{bytes} WHERE id=1",h.applied,h.applied);
 exec(src,&sql).map_err(r366_error)?;
 dest_tx.commit().map_err(r366_error)?;
 source_tx.commit().map_err(r366_error)?;
 Ok((acked,now.retained-acked))
}

#[test]fn r924_original_17_table_mutations_denied_on_both_private_databases(){
 let (root,h,_)=pair(2);let v=r586_oracle();
 r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
 // Both copies need an existing row, otherwise SQLite UPDATE is a no-op.
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 for db in [&h.source,&h.sink]{
  assert!(exec(db,"UPDATE events SET category='invalid'
   WHERE event_id=(SELECT MIN(event_id) FROM events)").is_err());
  assert!(exec(db,"DELETE FROM events").is_err());
  assert!(exec(db,"INSERT INTO metadata(key,value)
   VALUES('r924_nope','sneak')").is_err());
 }
 assert_eq!(r925_guarded_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,
   "r879_probe",||Ok(())),Ok((1,1)));
}
#[test]fn r925_new_paper_append_during_snapshot_does_not_invalidate_prefix(){
 let (root,h,_)=pair(3);let v=r586_oracle();
 r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 let writer=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let writer_sink=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 let ops=(97..=128).map(|i|{
  let (frame,update)=event(i);
  r824_original_paper_op(&v,"r879_probe",4,(i-97) as usize,32,&frame,&update).unwrap()
 }).collect::<Vec<_>>();
 let result=r925_guarded_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,
  "r879_probe",||{
    let n=r830_incremental_original_append(&root.0,&writer,&writer_sink,&v,&ops)?;
    assert_eq!(n,4);Ok(())
  });
 assert_eq!(result,Ok((1,3)),"safe append should NOT starve previously verified prefix");
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,3),Ok(3));
 assert_eq!(r925_guarded_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,
  "r879_probe",||Ok(())),Ok((3,0)));
 assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(128));
}
#[test]fn r926_preexisting_outbox_poison_is_fail_closed(){
 let (root,h,_)=pair(3);let v=r586_oracle();
 r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 exec(&h.source,"UPDATE mirror_outbox SET payload_hash='forged'
  WHERE seq=3").unwrap();
 assert_eq!(r925_guarded_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,
  "r879_probe",||Ok(())),Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
#[test]fn r927_source_schema_guard_removed_refuses_any_ack(){
 let (root,h,_)=pair(2);let v=r586_oracle();
 r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 exec(&h.source,"DROP TRIGGER r924_guard_events_update").unwrap();
 assert_eq!(r925_guarded_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,
  "r879_probe",||Ok(())),Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
