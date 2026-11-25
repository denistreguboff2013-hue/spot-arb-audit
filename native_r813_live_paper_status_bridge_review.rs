// Astra P0-01 R813: ACTUAL installed PAPER status -> Rust-owned ORIGINAL
// 17-table private DB1/outbox -> R805 Rust DB2. Limited to public PAPER
// aggregate status; NOT the legacy raw collector or real trading.
// Source path is attested against the installed supervisor's session manifest.
const R813_PAPER_ROOT:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1";
const R813_KEY:&str="r813_public_paper_observer_status";
fn r813_validate_paper_snapshot(v:&serde_json::Value,now_ms:u64)
 ->Result<String,EventOutboxR366Error>{
 let updated=r726_u(v,"updated_wall_ms")?;
 if updated>now_ms.saturating_add(15_000)
  ||now_ms.saturating_sub(updated)>180_000
  ||v["live_trading"]!=false||v["private_api"]!=false
  ||v["paper_portfolio"]["no_real_or_private_api"]!=true
  ||v["paper_portfolio"]["real_profit_proven"]!=false
  ||v["paper_portfolio"]["paper_fill_attested"]!=false{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let frames=r726_u(v,"frames")?;
 let decisions=r726_u(v,"decisions")?;
 let opportunities=r726_u(v,"opportunities")?;
 let entries=r726_u(&v["paper_portfolio"]["counters"],"entries")?;
 let closed=r726_u(&v["paper_portfolio"]["counters"],"closed")?;
 if decisions>frames||opportunities>decisions||closed>entries
  ||entries>10_000_000||frames>1_000_000_000{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let summary=serde_json::json!({
  "schema":"SPOTARB/R813/LIVE-PAPER-PUBLIC-STATUS/V1",
  "updated_wall_ms":updated,"frames":frames,"decisions":decisions,
  "opportunities":opportunities,"paper_entries":entries,
  "paper_closed":closed,
  "paper_open":r726_u(&v["paper_portfolio"],"open_positions")?,
  "modeled_only":true,"paper_fill_attested":false,
  "real_profit_proven":false,"real_orders_allowed":false
 });
 r386_python_mirror_canonical(&summary)
}
fn r813_attested_current_paper_status()
 ->Result<serde_json::Value,EventOutboxR366Error>{
 use std::os::unix::fs::MetadataExt;
 let base=Path::new(R813_PAPER_ROOT);
 let current=base.join("current_session.json");
 let cs=fs::symlink_metadata(&current).map_err(|_|EventOutboxR366Error::Path)?;
 if !cs.file_type().is_file()||cs.len()>16_384||cs.len()==0
  ||cs.uid()!=unsafe{libc::geteuid()}{return Err(EventOutboxR366Error::Path)}
 let j:serde_json::Value=serde_json::from_slice(
   &fs::read(&current).map_err(|_|EventOutboxR366Error::Path)?)
   .map_err(|_|EventOutboxR366Error::Corrupt)?;
 if j["paper_only"]!=true||j["virtual_positions_not_exchange_attested"]!=true{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let name=r726_s(&j,"session")?;
 if name.len()>90||!name.starts_with("session_")
  ||!name.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_'){
  return Err(EventOutboxR366Error::Path)
 }
 let folder=base.join("sessions").join(name);
 let f=fs::symlink_metadata(&folder).map_err(|_|EventOutboxR366Error::Path)?;
 if !f.file_type().is_dir(){return Err(EventOutboxR366Error::Path)}
 let status=folder.join("status.json");
 if r726_s(&j,"status_path")?!=status.to_string_lossy(){
  return Err(EventOutboxR366Error::Path)
 }
 let m=fs::symlink_metadata(&status).map_err(|_|EventOutboxR366Error::Path)?;
 if !m.file_type().is_file()||m.len()>256*1024||m.len()==0
  ||m.uid()!=unsafe{libc::geteuid()}{return Err(EventOutboxR366Error::Path)}
 serde_json::from_slice(&fs::read(status).map_err(|_|EventOutboxR366Error::Path)?)
  .map_err(|_|EventOutboxR366Error::Corrupt)
}

pub fn r813_private_initialize_once(root:&Path)->Result<(),String>{
 // Existing DBs never overwritten; callers must create their own 0700
 // private temp fixture directory before invoking this entrypoint.
 let pair=r756_initialize(root)
  .map_err(|e|format!("new isolated collector pair: {e:?}"))?;
 drop(pair);
 Ok(())
}
fn r813_append_public_summary(root:&Path,value:&str)
 ->Result<(bool,u64),EventOutboxR366Error>{
 if value.len()>4096||!value.contains("SPOTARB/R813/"){
  return Err(EventOutboxR366Error::Corrupt)
 }
 let (source,dest)=(
  r671_open(root,"r671-private-db1.sqlite",false)?,
  r671_open(root,"r671-private-db2.sqlite",false)?
 );
 let v=r586_oracle();
 let mut existing=Stmt::new(&source,
  "SELECT value FROM metadata WHERE key='r813_public_paper_observer_status'")
  .map_err(r366_error)?;
 let present=match existing.step().map_err(r366_error)?{
  SQLITE_ROW=>Some(existing.strval(0).map_err(r366_error)?),
  SQLITE_DONE=>None,
  _=>return Err(EventOutboxR366Error::Corrupt)
 };
 drop(existing);
 let latest=r366_query_num(&source,
  "SELECT latest_seq FROM mirror_control WHERE id=1")?;
 if present.as_deref()==Some(value){return Ok((false,latest))}
 let row=serde_json::json!({"key":R813_KEY,"value":value});
 let op=r659_op(&v,"metadata",&row,
  if present.is_some(){"update"}else{"insert"})?;
 let seq=r756_append(root,&source,&dest,&v,&[op])?;
 Ok((true,seq))
}
pub fn r813_private_ingest_installed_paper_once(root:&Path)
 ->Result<(bool,u64,u64,u64),String>{
 let status=r813_attested_current_paper_status()
  .map_err(|e|format!("attested PAPER source: {e:?}"))?;
 let now=std::time::SystemTime::now()
  .duration_since(std::time::UNIX_EPOCH)
  .map_err(|e|format!("clock: {e}"))?.as_millis() as u64;
 let summary=r813_validate_paper_snapshot(&status,now)
  .map_err(|e|format!("current PAPER status: {e:?}"))?;
 let (appended,seq)=r813_append_public_summary(root,&summary)
  .map_err(|e|format!("isolated Rust analytics DB1: {e:?}"))?;
 let (delivered,acknowledged,remaining)=r805_private_service_once(root)?;
 if remaining!=0{return Err("isolated PAPER replica has pending frames".into())}
 Ok((appended,seq,delivered,acknowledged))
}
#[cfg(test)]
mod r813_tests {
 use super::*;
 use serde_json::json;
 fn fixture()->serde_json::Value{
  json!({"updated_wall_ms":100000,"frames":420,"decisions":400,
  "opportunities":8,"live_trading":false,"private_api":false,
  "paper_portfolio":{"no_real_or_private_api":true,
   "real_profit_proven":false,"paper_fill_attested":false,
   "open_positions":1,"counters":{"entries":3,"closed":2}}})
 }
 #[test]fn r813_public_status_is_bounded_and_not_attested_real(){
  let x=r813_validate_paper_snapshot(&fixture(),100100).unwrap();
  assert!(x.contains("modeled_only"));
  assert!(x.contains("real_orders_allowed"));
  assert!(!x.contains("realized_usdt"));
 }
 #[test]fn r814_reject_private_api_even_with_valid_frames(){
  let mut x=fixture();x["private_api"]=json!(true);
  assert_eq!(r813_validate_paper_snapshot(&x,100100),
   Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r815_reject_stale_status_and_old_session_replay(){
  let x=fixture();
  assert_eq!(r813_validate_paper_snapshot(&x,500000),
   Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r816_reject_unproven_paper_fill(){
  let mut x=fixture();x["paper_portfolio"]["paper_fill_attested"]=json!(true);
  assert_eq!(r813_validate_paper_snapshot(&x,100100),
   Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r817_reject_inconsistent_counters(){
  let mut x=fixture();x["decisions"]=json!(421);
  assert_eq!(r813_validate_paper_snapshot(&x,100100),
   Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r818_rust_paper_analytics_source_to_two_real_sqlite_files(){
  use std::os::unix::fs::DirBuilderExt;
  let mut root=std::env::temp_dir();
  let stamp=std::time::SystemTime::now()
   .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
  root.push(format!("astra_r671_file_{}_{}_r818",std::process::id(),stamp));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  struct Owned(std::path::PathBuf);
  impl Drop for Owned{
   fn drop(&mut self){let _=std::fs::remove_dir_all(&self.0);}
  }
  let _owned=Owned(root.clone());
  r813_private_initialize_once(&root).unwrap();
  let first=r813_validate_paper_snapshot(&fixture(),100100).unwrap();
  assert_eq!(r813_append_public_summary(&root,&first),Ok((true,1)));
  assert_eq!(r805_private_service_once(&root),Ok((1,1,0)));
  assert_eq!(r813_append_public_summary(&root,&first),Ok((false,1)));
  let mut second=fixture();
  second["updated_wall_ms"]=json!(160000);
  second["frames"]=json!(421);second["decisions"]=json!(401);
  let updated=r813_validate_paper_snapshot(&second,160100).unwrap();
  assert_eq!(r813_append_public_summary(&root,&updated),Ok((true,2)));
  assert_eq!(r805_private_service_once(&root),Ok((1,1,0)));
  let a=r671_open(&root,"r671-private-db1.sqlite",false).unwrap();
  let b=r671_open(&root,"r671-private-db2.sqlite",false).unwrap();
  let v=r586_oracle();
  assert_eq!(r800_streaming_original_parity(&a,&b,&v),Ok(1));
  assert_eq!(r366_query_num(&a,"SELECT latest_seq FROM mirror_control"),Ok(2));
  assert_eq!(r366_query_num(&b,"SELECT applied_seq FROM mirror_meta"),Ok(2));
  assert_eq!(r366_query_num(&a,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
 }
}
